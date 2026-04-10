#include "pixz.h"

#include <archive.h>
#include <archive_entry.h>
#include <signal.h>


#pragma mark DECLARE WANTED

typedef struct wanted_t wanted_t;
struct wanted_t {
    wanted_t *next;
    char *name;
    off_t start, end;
    off_t size;
};

static wanted_t *gWantedFiles = NULL;

static bool spec_match(char *spec, char *name);
static void wanted_files(size_t count, char **specs);
static void wanted_free(wanted_t *w);


#pragma mark DECLARE PIPELINE

typedef enum { BLOCK_SIZED, BLOCK_UNSIZED, BLOCK_CONTINUATION } block_type;

typedef struct {
    uint8_t *input, *output;
	size_t incap, outcap;
    size_t insize, outsize;
    off_t uoffset; // uncompressed offset
	lzma_check check;
	
	block_type btype;
} io_block_t;

static void *block_create(void);
static void block_free(void *data);
static void read_thread(void);
static void read_thread_noindex(void);
static void decode_thread(size_t thnum);


#pragma mark DECLARE ARCHIVE

static pipeline_item_t *gArItem = NULL, *gArLastItem = NULL;
static off_t gArLastOffset;
static size_t gArLastSize;
static wanted_t *gArWanted = NULL;
static bool gArNextItem = false;
static bool gExplicitFiles = false;

static int tar_ok(struct archive *ar, void *ref);
static ssize_t tar_read(struct archive *ar, void *ref, const void **bufp);
static bool tar_next_block(void);
static void tar_write_last(void);


#pragma mark DECLARE READ BUFFER

#define STREAMSIZE (1024 * 1024)
#define MAXSPLITSIZE ((64 * 1024 * 1024) * 2) // xz -9 blocksize * 2

static pipeline_item_t *gRbufPI = NULL;
static io_block_t *gRbuf = NULL;

static void block_capacity(io_block_t *ib, size_t incap, size_t outcap);

typedef enum {
	RBUF_ERR, RBUF_EOF, RBUF_PART, RBUF_FULL
} rbuf_read_status;

static rbuf_read_status rbuf_read(size_t bytes);
static bool rbuf_cycle(lzma_stream *stream, bool start, size_t skip);
static void rbuf_consume(size_t bytes);
static void rbuf_dispatch(size_t bytes);

static bool read_header(lzma_check *check);
static bool read_block(bool force_stream, lzma_check check, off_t uoffset);
static void read_streaming(lzma_block *block, block_type sized, off_t uoffset);
static void read_index(void);
static void read_footer(void);


#pragma mark DECLARE UTILS

static lzma_vli gFileIndexOffset = 0;

static bool taste_tar(io_block_t *ib);
static bool taste_file_index(io_block_t *ib);


#pragma mark MAIN

void pixz_read(bool verify, size_t nspecs, char **specs) {
    if (decode_index()) {
	    if (verify)
	        gFileIndexOffset = read_file_index();
	    wanted_files(nspecs, specs);
		gExplicitFiles = nspecs;
    }

#if DEBUG
    for (wanted_t *w = gWantedFiles; w; w = w->next)
        debug("want: %s", w->name);
#endif
    
    pipeline_create(block_create, block_free,
		gIndex ? read_thread : read_thread_noindex, decode_thread);
    if (verify && gFileIndexOffset) {
        gArWanted = gWantedFiles;
        wanted_t *w = gWantedFiles, *wlast = NULL;
        bool lastmulti = false;
        off_t lastoff = 0;
        
        struct archive *ar = archive_read_new();
        prevent_compression(ar);
        archive_read_support_format_tar(ar);
        archive_read_open(ar, NULL, tar_ok, tar_read, tar_ok);
        struct archive_entry *entry;
        while (true) {
            int aerr = archive_read_next_header(ar, &entry);
            if (aerr == ARCHIVE_EOF) {
                break;
            } else if (aerr != ARCHIVE_OK && aerr != ARCHIVE_WARN) {
                fprintf(stderr, "%s\n", archive_error_string(ar));
                die("Error reading archive entry");
            }
            
            off_t off = archive_read_header_position(ar);
            const char *path = archive_entry_pathname(entry);
            if (!lastmulti) {
                if (wlast && wlast->size != off - lastoff)
                    die("Index and archive show differing sizes for %s: %jd vs %jd",
                        wlast->name, (intmax_t)wlast->size, (intmax_t)(off - lastoff));
                lastoff = off;
            }
            
            lastmulti = is_multi_header(path);
            if (lastmulti)
                continue;
            
            if (!w)
                die("File %s missing in index", path);
            if (strcmp(path, w->name) != 0)
                die("Index and archive differ as to next file: %s vs %s",
                    w->name, path);
            
            wlast = w;
            w = w->next;
        }
		finish_reading(ar);
        if (w && w->name)
            die("File %s missing in archive", w->name);
        tar_write_last(); // write whatever's left
    }
	if (!gExplicitFiles) {
		/* Heuristics for detecting pixz file index:
		 *    - Input must be streaming (otherwise read_thread does this) 
		 *    - Data must look tar-like
		 *    - Must have all sized blocks, followed by unsized file index */
		bool start = !gIndex && verify,
			 tar = false, all_sized = true, skipping = false;
		
		pipeline_item_t *pi;
        while ((pi = pipeline_merged())) {
            io_block_t *ib = (io_block_t*)(pi->data);
			if (skipping && ib->btype != BLOCK_CONTINUATION) {
				fprintf(stderr,
					"Warning: File index heuristic failed, use -t flag.\n");
				skipping = false;
			}
			if (!skipping && tar && !start && all_sized
					&& ib->btype == BLOCK_UNSIZED && taste_file_index(ib))
				skipping = true;
			if (start) {
				tar = taste_tar(ib);
				start = false;
			}
			if (ib->btype == BLOCK_UNSIZED)
				all_sized = false;
			
			if (!skipping) {
				if (fwrite(ib->output, ib->outsize, 1, gOutFile) != 1)
					die("Can't write block");
			}
            queue_push(gPipelineStartQ, PIPELINE_ITEM, pi);
        }
    }
    
    pipeline_destroy();
    wanted_free(gWantedFiles);
}


#pragma mark BLOCKS

static void *block_create(void) {
    io_block_t *ib = xmalloc(sizeof(io_block_t));
	ib->incap = ib->outcap = 0;
	ib->input = ib->output = NULL;
    return ib;
}

static void block_free(void* data) {
    io_block_t *ib = (io_block_t*)data;
    free(ib->input);
    free(ib->output);
    free(ib);
}


#pragma mark SETUP

static void wanted_free(wanted_t *w) {
    for (wanted_t *w = gWantedFiles; w; ) {
        wanted_t *tmp = w->next;
        free(w);
        w = tmp;
    }
}


static bool spec_match(char *spec, char *name) {
    bool match = true;
    for (; *spec; ++spec, ++name) {
        if (!*name || *spec != *name) { // spec must be equal or prefix
            match = false;
            break;
        }
    }
    // If spec's a prefix of the file name, it must be a dir name
    return match && (!*name || *name == '/');
}

static void wanted_files(size_t count, char **specs) {
    if (!gFileIndexOffset) {
        if (count)
            die("Can't filter non-tarball");
        gWantedFiles = NULL;
        return;
    }
    
    // Remove trailing slashes from specs
    for (char **spec = specs; spec < specs + count; ++spec) {
        char *c = *spec;
        while (*c++) ; // forward to end
        while (--c >= *spec && *c == '/')
            *c = '\0';
    }
    
    bool matched[count];  // for each spec, does it match?
    memset(matched, 0, sizeof(matched));
    wanted_t *last = NULL;
    
    // Check each file in order, to see if we want it
    for (file_index_t *f = gFileIndex; f->name; f = f->next) {
        bool match = !count;
        for (char **spec = specs; spec < specs + count; ++spec) {
            if (spec_match(*spec, f->name)) {
                match = true;
                matched[spec - specs] = true;
                break;
            }
        }
        
        if (match) {
            wanted_t *w = xmalloc(sizeof(wanted_t));
            *w = (wanted_t){ .name = f->name, .start = f->offset,
                .end = f->next->offset, .next = NULL };
            w->size = w->end - w->start;
            if (last) {
                last->next = w;
            } else {
                gWantedFiles = w;
            }
            last = w;
        }
    }
    
    // Make sure each spec matched
    for (size_t i = 0; i < count; ++i) {
        if (!matched[i])
            die("\"%s\" not found in archive", *(specs + i));
    }
}


#pragma mark READ

static void block_capacity(io_block_t *ib, size_t incap, size_t outcap) {
	if (incap > ib->incap) {
		ib->incap = incap;
		ib->input = realloc(ib->input, incap);
	}
	if (outcap > ib->outcap) {
		ib->outcap = outcap;
		ib->output = xmalloc(outcap);
	}
}

// Get the next rbuf from the pipeline, and put it in gRbuf
static void rbuf_from_pipeline(void) {
    queue_pop(gPipelineStartQ, (void**)&gRbufPI);
    gRbuf = (io_block_t*)(gRbufPI->data);
    gRbuf->insize = gRbuf->outsize = 0;
}

// Ensure at least this many bytes available
// Return 1 on success, zero on EOF, -1 on error
static rbuf_read_status rbuf_read(size_t bytes) {
	if (!gRbufPI) {
        rbuf_from_pipeline();
	}
	
	if (gRbuf->insize >= bytes)
		return RBUF_FULL;
	
	block_capacity(gRbuf, bytes, 0);
	size_t r = fread(gRbuf->input + gRbuf->insize, 1, bytes - gRbuf->insize,
		gInFile);
	gRbuf->insize += r;
	
	if (r)
		return (gRbuf->insize == bytes) ? RBUF_FULL : RBUF_PART;
	return feof(gInFile) ? RBUF_EOF : RBUF_ERR;
}

static bool rbuf_cycle(lzma_stream *stream, bool start, size_t skip) {
	if (!start) {
		rbuf_consume(gRbuf->insize);
		if (rbuf_read(CHUNKSIZE) < RBUF_PART)
			return false;
	}
	stream->next_in = gRbuf->input + skip;
	stream->avail_in = gRbuf->insize - skip;
	return true;
}

static void rbuf_consume(size_t bytes) {
	if (bytes < gRbuf->insize)
		memmove(gRbuf->input, gRbuf->input + bytes, gRbuf->insize - bytes);
	gRbuf->insize -= bytes;
}

static void rbuf_dispatch(size_t total_size) {
    pipeline_item_t *prev_pi = gRbufPI;
    if (gRbuf->insize > total_size) {
        // We have extra data, get a place for it to live
        io_block_t *prev_rbuf = gRbuf;
        rbuf_from_pipeline();
        size_t extra = prev_rbuf->insize - total_size;
        block_capacity(gRbuf, extra, 0);
        memcpy(gRbuf->input, prev_rbuf->input + total_size, extra);
        gRbuf->insize = extra;
    } else {
        gRbufPI = NULL;
        gRbuf = NULL;
    }

	pipeline_split(prev_pi);
}


static bool read_header(lzma_check *check) {
	lzma_stream_flags stream_flags;
	rbuf_read_status st = rbuf_read(LZMA_STREAM_HEADER_SIZE);
	if (st == RBUF_EOF)
		return false;
	else if (st != RBUF_FULL)
		die("Error reading stream header");
	lzma_ret err = lzma_stream_header_decode(&stream_flags, gRbuf->input);
	if (err == LZMA_FORMAT_ERROR)
		die("Not an XZ file");
	else if (err != LZMA_OK)
		die("Error decoding XZ header");
	*check = stream_flags.check;
	rbuf_consume(LZMA_STREAM_HEADER_SIZE);
	return true;
}

static bool read_block(bool force_stream, lzma_check check, off_t uoffset) {
    lzma_filter filters[LZMA_FILTERS_MAX + 1];
    lzma_block block = { .filters = filters, .check = check, .version = 0 };
	
	if (rbuf_read(1) != RBUF_FULL)
		die("Error reading block header size");
	if (gRbuf->input[0] == 0)
		return false;
	
	block.header_size = lzma_block_header_size_decode(gRbuf->input[0]);
	if (block.header_size > LZMA_BLOCK_HEADER_SIZE_MAX)
		die("Block header size too large");
	if (rbuf_read(block.header_size) != RBUF_FULL)
		die("Error reading block header");
	if (lzma_block_header_decode(&block, NULL, gRbuf->input) != LZMA_OK)
		die("Error decoding block header");
		
	size_t comp = block.compressed_size, outsize = block.uncompressed_size;
	bool sized = (comp != LZMA_VLI_UNKNOWN && outsize != LZMA_VLI_UNKNOWN);
    if (force_stream || !sized || outsize > MAXSPLITSIZE) {
		read_streaming(&block, sized ? BLOCK_SIZED : BLOCK_UNSIZED, uoffset);
	} else {
		block_capacity(gRbuf, 0, outsize);
		gRbuf->outsize = outsize;
		gRbuf->check = check;
		gRbuf->btype = BLOCK_SIZED;
		
        size_t total_size = lzma_block_total_size(&block);
		if (rbuf_read(total_size) != RBUF_FULL)
			die("Error reading block contents");
		rbuf_dispatch(total_size);
	}
	return true;
}

static void read_streaming(lzma_block *block, block_type sized, off_t uoffset) {
    lzma_stream stream = LZMA_STREAM_INIT;
    if (lzma_block_decoder(&stream, block) != LZMA_OK)
		die("Error initializing streaming block decode");
	rbuf_cycle(&stream, true, block->header_size);
	stream.avail_out = 0;
	
	bool first = true;
    pipeline_item_t *pi = NULL;
    io_block_t *ib = NULL;
    
	lzma_ret err = LZMA_OK;
	while (err != LZMA_STREAM_END) {
		if (err != LZMA_OK)
			die("Error decoding streaming block");
		
		if (stream.avail_out == 0) {
			if (ib) {
				ib->outsize = ib->outcap;
                ib->uoffset = uoffset;
                uoffset += ib->outsize;
				pipeline_dispatch(pi, gPipelineMergeQ);
				first = false;
			}
			queue_pop(gPipelineStartQ, (void**)&pi);
			ib = (io_block_t*)pi->data;
			ib->btype = (first ? sized : BLOCK_CONTINUATION);
			block_capacity(ib, 0, STREAMSIZE);
			stream.next_out = ib->output;
			stream.avail_out = ib->outcap;
		}
		if (stream.avail_in == 0 && !rbuf_cycle(&stream, false, 0))
			die("Error reading streaming block");
		
		err = lzma_code(&stream, LZMA_RUN);
	}
	
	if (ib && stream.avail_out != ib->outcap) {
		ib->outsize = ib->outcap - stream.avail_out;
		pipeline_dispatch(pi, gPipelineMergeQ);
	}
	rbuf_consume(gRbuf->insize - stream.avail_in);
	lzma_end(&stream);
}

static void read_index(void) {
    lzma_stream stream = LZMA_STREAM_INIT;
	lzma_index *index;
	if (lzma_index_decoder(&stream, &index, MEMLIMIT) != LZMA_OK)
		die("Error initializing index decoder");
	rbuf_cycle(&stream, true, 0);
	
	lzma_ret err = LZMA_OK;
	while (err != LZMA_STREAM_END) {
		if (err != LZMA_OK)
			die("Error decoding index");
		if (stream.avail_in == 0 && !rbuf_cycle(&stream, false, 0))
			die("Error reading index");
		err = lzma_code(&stream, LZMA_RUN);
	}
	rbuf_consume(gRbuf->insize - stream.avail_in);
	lzma_end(&stream);
}

static void read_footer(void) {
	lzma_stream_flags stream_flags;
	if (rbuf_read(LZMA_STREAM_HEADER_SIZE) != RBUF_FULL)
		die("Error reading stream footer");
	if (lzma_stream_footer_decode(&stream_flags, gRbuf->input) != LZMA_OK)
		die("Error decoding XZ footer");
	rbuf_consume(LZMA_STREAM_HEADER_SIZE);
	
	char zeros[4] = "\0\0\0\0";
	while (true) {
		rbuf_read_status st = rbuf_read(4);
		if (st == RBUF_EOF)
			return;
		if (st != RBUF_FULL)
			die("Footer must be multiple of four bytes");
		if (memcmp(zeros, gRbuf->input, 4) != 0)
			return;
		rbuf_consume(4);
	}
}

static void read_thread_noindex(void) {
	bool empty = true;
	lzma_check check = LZMA_CHECK_NONE;
	while (read_header(&check)) {
		empty = false;
		while (read_block(false, check, 0))
			; // pass
		read_index();
		read_footer();
	}
	if (empty)
		die("Empty input");
	pipeline_stop();
}

static void read_thread(void) {
    off_t offset = ftello(gInFile);
    wanted_t *w = gWantedFiles;
    
    lzma_index_iter iter;
    lzma_index_iter_init(&iter, gIndex);
    while (!lzma_index_iter_next(&iter, LZMA_INDEX_ITER_BLOCK)) {
        // Don't decode the file-index
        off_t boffset = iter.block.compressed_file_offset;
        size_t bsize = iter.block.total_size;
        if (gFileIndexOffset && boffset == gFileIndexOffset)
            continue;
        
        // Do we need this block?
        if (gWantedFiles && gExplicitFiles) {
            off_t uend = iter.block.uncompressed_file_offset +
                iter.block.uncompressed_size;
            if (!w || w->start >= uend) {
                debug("read: skip %llu", iter.block.number_in_file);
                continue;
            }
            for ( ; w && w->end <= uend; w = w->next) ;
        }
        debug("read: want %llu", iter.block.number_in_file);
        
        // Seek if needed, and get the data
        if (offset != boffset) {
            fseeko(gInFile, boffset, SEEK_SET);
            offset = boffset;
        }
		
		if (iter.block.uncompressed_size > MAXSPLITSIZE) { // must stream
			if (gRbuf)
				rbuf_consume(gRbuf->insize); // clear
			read_block(true, iter.stream.flags->check,
                iter.block.uncompressed_file_offset);
		} else {
            // Get a block to work with
            pipeline_item_t *pi;
            queue_pop(gPipelineStartQ, (void**)&pi);
            io_block_t *ib = (io_block_t*)(pi->data);
            block_capacity(ib, bsize,
                iter.block.uncompressed_size);
            
	        ib->insize = fread(ib->input, 1, bsize, gInFile);
	        if (ib->insize < bsize)
	            die("Error reading block contents");
	        offset += bsize;
	        ib->uoffset = iter.block.uncompressed_file_offset;
			ib->check = iter.stream.flags->check;
			ib->btype = BLOCK_SIZED; // Indexed blocks always sized
			
	        pipeline_split(pi);
		}
    }
    
    pipeline_stop();
}

#pragma mark DECODE

static void decode_thread(size_t thnum) {
    lzma_stream stream = LZMA_STREAM_INIT;
    lzma_filter filters[LZMA_FILTERS_MAX + 1];
    lzma_block block = { .filters = filters, .check = LZMA_CHECK_NONE,
		.version = 0 };
    
    pipeline_item_t *pi;
    io_block_t *ib;
    
    while (PIPELINE_STOP != queue_pop(gPipelineSplitQ, (void**)&pi)) {
        ib = (io_block_t*)(pi->data);
        
        block.header_size = lzma_block_header_size_decode(*(ib->input));
        block.check = ib->check;
		if (lzma_block_header_decode(&block, NULL, ib->input) != LZMA_OK)
            die("Error decoding block header");
        if (lzma_block_decoder(&stream, &block) != LZMA_OK)
            die("Error initializing block decode");
        
        stream.avail_in = ib->insize - block.header_size;
        stream.next_in = ib->input + block.header_size;
        stream.avail_out = ib->outcap;
        stream.next_out = ib->output;
        
        lzma_ret err = LZMA_OK;
        while (err != LZMA_STREAM_END) {
            if (err != LZMA_OK)
                die("Error decoding block");
            err = lzma_code(&stream, LZMA_FINISH);
        }
        
        ib->outsize = stream.next_out - ib->output;
        queue_push(gPipelineMergeQ, PIPELINE_ITEM, pi);
    }
    lzma_end(&stream);
}


#pragma mark ARCHIVE

static int tar_ok(struct archive *ar, void *ref) {
    return ARCHIVE_OK;
}

static bool tar_next_block(void) {
    if (gArItem && !gArNextItem && gArWanted && gExplicitFiles) {
        io_block_t *ib = (io_block_t*)(gArItem->data);
        if (gArWanted->start < ib->uoffset + ib->outsize)
            return true; // No need
    }
    
    if (gArLastItem)
        queue_push(gPipelineStartQ, PIPELINE_ITEM, gArLastItem);
    gArLastItem = gArItem;
    gArItem = pipeline_merged();
    gArNextItem = false;
    return gArItem;
}

static void tar_write_last(void) {
    if (gArItem) {
        io_block_t *ib = (io_block_t*)(gArItem->data);
        if (fwrite(ib->output + gArLastOffset, gArLastSize, 1, gOutFile) != 1)
			die("Can't write previous block");
        gArLastSize = 0;
    }
}

static ssize_t tar_read(struct archive *ar, void *ref, const void **bufp) {
    // If we got here, the last bit of archive is ok to write
    tar_write_last();
        
    // Write the first wanted file
    if (!tar_next_block())
        return 0;
    
    off_t off;
    off_t size;
    io_block_t *ib = (io_block_t*)(gArItem->data);
    if (gWantedFiles && gExplicitFiles) {
        debug("tar want: %s", gArWanted->name);
        off = gArWanted->start - ib->uoffset;
        size = gArWanted->size;
        if (off < 0) {
            size += off;
            off = 0;
        }
        if (off + size > ib->outsize) {
            size = ib->outsize - off;
            gArNextItem = true; // force the end of this block
        } else {
            gArWanted = gArWanted->next;
        }
    } else {
        off = 0;
        size = ib->outsize;
    }
    debug("tar off = %llu, size = %zu", (unsigned long long)off, size);
    
    gArLastOffset = off;
    gArLastSize = size;
    if (bufp)
        *bufp = ib->output + off;
    return size;
}


#pragma mark UTILS

static bool taste_tar(io_block_t *ib) {
    struct archive *ar = archive_read_new();
    prevent_compression(ar);
    archive_read_support_format_tar(ar);
    archive_read_open_memory(ar, ib->output, ib->outsize);
    struct archive_entry *entry;
    bool ok = (archive_read_next_header(ar, &entry) == ARCHIVE_OK);
	finish_reading(ar);
	return ok;
}

static bool taste_file_index(io_block_t *ib) {
	return xle64dec(ib->output) == PIXZ_INDEX_MAGIC;
}


#pragma mark SORTED EXTRACT

#define TAR_BLOCK_SIZE  512     /* size of one tar header/data block in bytes */
#define ROUND_UP_TO_TAR_BLOCK(n) \
    (((size_t)(n) + TAR_BLOCK_SIZE - 1) / TAR_BLOCK_SIZE * TAR_BLOCK_SIZE)

/* Return the extension (including the leading dot) from the last path
 * component.  Uses the *last non-numeric* dot component so that:
 *  - Dots in version-number stems (e.g. "libstdc++6-4.4-dev_4.4.5-8_i386.deb")
 *    do not corrupt extension grouping — ".deb" is returned.
 *  - Shared-library version suffixes (".so.1", ".so.1.2.3") are skipped and
 *    the real extension ".so" is returned.
 *  - For compressed files the inner format is ignored — "archive.tar.gz" and
 *    "image.cpio.gz" both return ".gz", because the compression format
 *    dominates compressibility.
 *  - Trailing backup/metadata suffixes are stripped first so that e.g.
 *    "foo.c.bak" and "foo.c~" group with ".c" files.  Stripped suffixes:
 *      .svn-base           Subversion pristine-copy marker
 *      .bak  ~  .old       generic editor/tool backup suffixes
 *      .orig  .rej  .new  patch-workflow originals, rejects, and replacements
 *      .dpkg-old  .dpkg-new  .dpkg-dist  .dpkg-bak   dpkg config handling
 *      .rpmsave  .rpmnew  .rpmorig        RPM config handling
 *      .pacnew  .pacsave                  pacman config handling
 *  - A leading dot (hidden files like ".gitignore") is not an extension.
 *
 * Returns an empty string if there is no extension. */

static const char *file_type_ext(const char *name) {
    if (!name) return "";

    /* Find the start of the last path component. */
    const char *slash = strrchr(name, '/');
    const char *base  = slash ? slash + 1 : name;

    /* Strip at most one trailing backup/metadata suffix so that e.g.
     * "foo.c.bak" groups with ".c".  Copy into a static buffer when stripping
     * is needed; the comparator that calls us is single-threaded. */
    static const char * const strip_sfx[] = {
        ".svn-base",   /* Subversion pristine-copy marker */
        ".bak",        /* generic backup */
        "~",           /* Emacs / editor tilde backup */
        ".old",        /* generic old-version backup */
        ".orig",       /* original file (patch workflow, Debian, etc.) */
        ".rej",        /* patch reject file */
        ".new",        /* new-version counterpart of .old/.orig */
        ".dpkg-old",   /* dpkg: replaced config file */
        ".dpkg-new",   /* dpkg: new config file not yet adopted */
        ".dpkg-dist",  /* dpkg: distributor's default config */
        ".dpkg-bak",   /* dpkg: backup of the previous config */
        ".rpmsave",    /* RPM: saved config replaced by package */
        ".rpmnew",     /* RPM: new config not yet adopted */
        ".rpmorig",    /* RPM: original config saved before first install */
        ".pacnew",     /* pacman: new config not yet adopted */
        ".pacsave",    /* pacman: saved config replaced by package */
        NULL
    };
    static char buf[4096];
    size_t blen = strlen(base);
    for (const char * const *sfx = strip_sfx; *sfx; sfx++) {
        size_t slen = strlen(*sfx);
        if (blen >= slen &&
                memcmp(base + blen - slen, *sfx, slen) == 0) {
            size_t tlen = blen - slen;
            if (tlen >= sizeof buf) tlen = sizeof buf - 1;
            memcpy(buf, base, tlen);
            buf[tlen] = '\0';
            base = buf;
            break;  /* strip at most one suffix */
        }
    }

    /* Scan backwards, tracking whether every character seen since the last dot
     * (or the end of the string) has been a digit.  When we reach a dot:
     *  - if the component to its right was non-numeric, this is the real
     *    extension — return it;
     *  - if it was purely numeric (a version-number suffix like ".1" or ".2.3"
     *    in "libfoo.so.1.2.3"), reset the flag and keep scanning left.
     * A leading dot (p == base) is never treated as an extension separator. */
    bool component_all_digits = true;
    for (const char *p = base + strlen(base) - 1; p > base; p--) {
        if (*p == '.') {
            if (!component_all_digits)
                return p;
            component_all_digits = true;  /* reset for the next component */
        } else {
            if (*p < '0' || *p > '9')
                component_all_digits = false;
        }
    }
    return "";
}

/* Counters for monitoring block (re-)decompression during sorted extract.
 * Printed to stderr at the end of pixz_sorted_extract when DEBUG is enabled. */
typedef struct {
    size_t decompressions;        /* first-time decompress calls (not redecompressions) */
    size_t cache_hits;            /* blocks served from the Bélády cache */
    size_t redecompressions;      /* blocks decompressed again after cache eviction */
    uint64_t decompressed_bytes;  /* uncompressed bytes from first-time decompressions */
    uint64_t redecompressed_bytes;/* uncompressed bytes from re-decompressions */
    uint64_t cache_hit_bytes;     /* uncompressed bytes served from cache */
} sort_stats_t;

/* Size threshold for classifying large files during sorted extract.
 * Files larger than this threshold are sorted last to avoid disrupting
 * runs of compressible small files.  Default is 8 MiB.
 * Set via the -D command-line option. */
size_t gSortDictSize = 8 * 1024 * 1024;

/* Bélády block cache size limits.  See pixz.h for semantics.
 * When gBcMaxBytes > 0 it overrides gBcMaxEntries (RAM mode).
 * gBcMaxEntries <= 0 (with gBcMaxBytes == 0) means unbounded. */
ssize_t gBcMaxEntries = 32;
size_t  gBcMaxBytes   = 0;

/* Compare two file paths right-to-left, one component at a time.
 * The comparison order is: basename, then the directory containing it,
 * then that directory's parent, all the way up to the root.
 *
 * This groups files that share a basename (e.g. "models.py") together
 * across different parent directories, then files in same-named directories
 * together, etc.  A shallower path (fewer components) sorts before a deeper
 * one when all trailing components are identical. */
static int cmp_path_reversed(const char *a, const char *ea,
                              const char *b, const char *eb) {
    /* Strip a trailing '/' from the current end pointer (safety; paths in a
     * tar index should not end with '/' for regular files, but be defensive). */
    while (ea > a && ea[-1] == '/') --ea;
    while (eb > b && eb[-1] == '/') --eb;

    /* Find the start of the rightmost component in each path. */
    const char *sa = ea;
    while (sa > a && sa[-1] != '/') --sa;
    const char *sb = eb;
    while (sb > b && sb[-1] != '/') --sb;

    /* Compare just this component. */
    size_t la = (size_t)(ea - sa);
    size_t lb = (size_t)(eb - sb);
    int r = strncmp(sa, sb, la < lb ? la : lb);
    if (r != 0) return r;
    if (la != lb) return (la < lb) ? -1 : 1;

    /* This component is equal.  Recurse into parent directories. */
    int has_parent_a = (sa > a);   /* sa[-1] == '/' if true */
    int has_parent_b = (sb > b);

    if (!has_parent_a && !has_parent_b) return 0;
    if (!has_parent_a) return -1;  /* a is shallower → sorts before b */
    if (!has_parent_b) return  1;

    /* Move end pointers back past the '/' separator and recurse. */
    return cmp_path_reversed(a, sa - 1, b, sb - 1);
}

static int cmp_sorted_files(const void *a, const void *b) {
    const file_index_t * const *fa = (const file_index_t * const *)a;
    const file_index_t * const *fb = (const file_index_t * const *)b;
    const char *na = (*fa)->name ? (*fa)->name : "";
    const char *nb = (*fb)->name ? (*fb)->name : "";

    off_t sa = (*fa)->next->offset - (*fa)->offset;
    off_t sb = (*fb)->next->offset - (*fb)->offset;

    /* Tier 0: header-only entries (directories, symlinks, hard links,
     * devices, FIFOs, and 0-byte regular files — all exactly one 512-byte
     * tar header block with no data blocks) sort FIRST.
     *
     * In a typical tar archive a directory entry appears immediately before
     * the files it contains, so the directory shares its lzma block with
     * those files.  Sorting directories first means the directory is always
     * an *early* consumer of the block; lu_last_user is determined by the
     * normal files in the block and is unchanged from the no-directory case.
     * Sorting directories last would make every such block stay in the cache
     * for the entire sort (lu_last_user = count-1), inflating cache pressure
     * with no benefit. */
    int ha = (sa <= TAR_BLOCK_SIZE);
    int hb = (sb <= TAR_BLOCK_SIZE);
    if (ha != hb) return hb - ha;   /* 1 (header-only) sorts before 0 */

    /* Both header-only: no data content, so extension grouping is pointless.
     * Sort by filename only for a stable, predictable order. */
    if (ha && hb) return strcmp(na, nb);

    /* Primary: small files (< threshold) sort before large files.
     * Large files flush the compressor dictionary entirely on their own, so
     * they gain nothing from adjacency to other files; isolating them prevents
     * them from breaking up runs of compressible content. */
    int la = (sa >= (off_t)gSortDictSize);
    int lb = (sb >= (off_t)gSortDictSize);
    if (la != lb) return la - lb;   /* 0 (small) sorts before 1 (large) */

    /* Secondary: group globally by extension.  This keeps all .c files
     * together, all .py files together, etc., across the whole archive,
     * building a richer compressor dictionary for each file type. */
    int r = strcmp(file_type_ext(na), file_type_ext(nb));
    if (r != 0) return r;

    /* Tertiary: within the same extension, compare path components
     * right-to-left (basename first, then parent directory, then
     * grandparent, up to the root).  This groups files with identical
     * basenames together across different top-level directories, then
     * files in same-named directories together, etc., maximising the
     * chance that the LZMA back-reference window can reach identical
     * content from a sibling container.  Shallower paths sort before
     * deeper ones when all right-aligned components are equal. */
    return cmp_path_reversed(na, na + strlen(na), nb, nb + strlen(nb));
}


/* ---- access-list lookup table -----------------------------------------
 *
 * For each lzma block (keyed by compressed offset) we record every
 * sorted-order file index that overlaps it.  Because the prepass iterates
 * i = 0 … count-1 in ascending order, appending gives a naturally sorted
 * array — no post-sort needed.
 *
 * Two queries are used:
 *   lu_last_user  – the highest index (= users[user_count-1]); used by
 *                   bc_evict_done() to know when a cached block is dead.
 *   lu_next_user  – the smallest index strictly greater than after_idx;
 *                   used by bc_evict_belady() to implement the true Bélády
 *                   policy (evict the block whose *next* use is furthest).
 */

#define LU_HASH_BITS 8
#define LU_HASH_SIZE (1 << LU_HASH_BITS)
#define LU_HASH_MASK (LU_HASH_SIZE - 1)

/* Shift the offset right by 3 bits before masking: lzma block offsets are
 * always aligned to at least 4 bytes, so the low bits carry no information.
 * The shift improves hash distribution across the buckets. */
#define LU_HASH(off)  ((size_t)((off) >> 3) & LU_HASH_MASK)

typedef struct lu_entry_t lu_entry_t;
struct lu_entry_t {
    lzma_vli   comp_off;
    size_t    *users;       /* sorted array of sorted-order indices */
    size_t     user_count;
    size_t     user_cap;
    lu_entry_t *next;       /* hash-chain */
    bool       seen;        /* true once this block has been decompressed */
};

typedef struct { lu_entry_t *b[LU_HASH_SIZE]; } lu_table_t;

static void lu_init(lu_table_t *t) { memset(t, 0, sizeof(*t)); }

/* Append user to the block's access list.  Caller must call with
 * non-decreasing user values so the list stays sorted.  Consecutive
 * duplicate values are silently dropped (a file can only use a block once). */
static void lu_add_access(lu_table_t *t, lzma_vli off, size_t user) {
    size_t h = LU_HASH(off);
    lu_entry_t *e;
    for (e = t->b[h]; e; e = e->next)
        if (e->comp_off == off) break;
    if (!e) {
        e = xmalloc(sizeof(lu_entry_t));
        e->comp_off = off;
        e->users = NULL;
        e->user_count = e->user_cap = 0;
        e->seen = false;
        e->next = t->b[h]; t->b[h] = e;
    }
    /* Drop consecutive duplicate. */
    if (e->user_count > 0 && e->users[e->user_count - 1] == user)
        return;
    if (e->user_count == e->user_cap) {
        e->user_cap = e->user_cap ? e->user_cap * 2 : 4;
        size_t *tmp = realloc(e->users, e->user_cap * sizeof(size_t));
        if (!tmp) die("Out of memory in lu_add_access");
        e->users = tmp;
    }
    e->users[e->user_count++] = user;
}

/* Returns the last (highest) accessing index, or SIZE_MAX if not found. */
static size_t lu_last_user(const lu_table_t *t, lzma_vli off) {
    size_t h = LU_HASH(off);
    for (const lu_entry_t *e = t->b[h]; e; e = e->next)
        if (e->comp_off == off)
            return e->user_count ? e->users[e->user_count - 1] : SIZE_MAX;
    return SIZE_MAX;
}

/* Returns the smallest accessing index strictly greater than after_idx,
 * or SIZE_MAX if there is none.  Used for the cache-bypass check: is this
 * block needed by any file AFTER the current one? */
static size_t lu_next_user(const lu_table_t *t, lzma_vli off, size_t after_idx) {
    size_t h = LU_HASH(off);
    for (const lu_entry_t *e = t->b[h]; e; e = e->next) {
        if (e->comp_off != off) continue;
        /* Binary search for first element > after_idx. */
        size_t lo = 0, hi = e->user_count;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (e->users[mid] <= after_idx) lo = mid + 1;
            else hi = mid;
        }
        return lo < e->user_count ? e->users[lo] : SIZE_MAX;
    }
    return SIZE_MAX;
}

/* Returns the smallest accessing index >= from_idx (inclusive),
 * or SIZE_MAX if there is none.  Used for the Bélády eviction key: a block
 * needed AT the current file index must not be evicted (its distance = 0). */
static size_t lu_next_user_from(const lu_table_t *t, lzma_vli off, size_t from_idx) {
    size_t h = LU_HASH(off);
    for (const lu_entry_t *e = t->b[h]; e; e = e->next) {
        if (e->comp_off != off) continue;
        /* Binary search for first element >= from_idx. */
        size_t lo = 0, hi = e->user_count;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (e->users[mid] < from_idx) lo = mid + 1;
            else hi = mid;
        }
        return lo < e->user_count ? e->users[lo] : SIZE_MAX;
    }
    return SIZE_MAX;
}

/* Returns true if this block has been decompressed at least once.
 * Returns false for blocks not in the table (single-user blocks, pruned out). */
static bool lu_is_seen(const lu_table_t *t, lzma_vli off) {
    size_t h = LU_HASH(off);
    for (const lu_entry_t *e = t->b[h]; e; e = e->next)
        if (e->comp_off == off) return e->seen;
    return false;
}

/* Mark a block as having been decompressed.  No-op if not in the table. */
static void lu_mark_seen(lu_table_t *t, lzma_vli off) {
    size_t h = LU_HASH(off);
    for (lu_entry_t *e = t->b[h]; e; e = e->next)
        if (e->comp_off == off) { e->seen = true; return; }
}

static void lu_free(lu_table_t *t) {
    for (int h = 0; h < LU_HASH_SIZE; ++h) {
        lu_entry_t *e = t->b[h];
        while (e) {
            lu_entry_t *n = e->next;
            free(e->users);
            free(e);
            e = n;
        }
        t->b[h] = NULL;
    }
}

/* Remove entries that are accessed by only one file.  Such blocks will never
 * be shared and need not be cached.  Absent entries return SIZE_MAX from all
 * lu queries, which is the correct "don't cache / no future user" signal. */
static void lu_prune_single_user(lu_table_t *t) {
    for (int h = 0; h < LU_HASH_SIZE; ++h) {
        lu_entry_t **pp = &t->b[h];
        while (*pp) {
            lu_entry_t *e = *pp;
            if (e->user_count < 2) {
                *pp = e->next;
                free(e->users);
                free(e);
            } else {
                pp = &e->next;
            }
        }
    }
}


/* ---- Bélády-optimal block cache ---------------------------------------
 *
 * Caches decompressed lzma blocks so that a block shared by several files
 * (in sorted order) is decompressed only once.  When the last consumer of
 * a block has been served, bc_evict_done() drops it immediately.  When the
 * cache is full and a new block must be inserted, bc_evict_belady() applies
 * the true Bélády policy: it consults the per-block access lists to find
 * each cached entry's *next* consumer after the current file index, then
 * evicts the entry whose next consumer is furthest ahead (or has no next
 * consumer).  This is provably optimal — no other eviction policy makes
 * fewer cache misses for the same cache size.
 */

#define BC_HASH_BITS    6
#define BC_HASH_SIZE    (1 << BC_HASH_BITS)
#define BC_HASH_MASK    (BC_HASH_SIZE - 1)
#define BC_HASH(off)    ((size_t)(off) & BC_HASH_MASK)

typedef struct bc_entry_t bc_entry_t;
struct bc_entry_t {
    lzma_vli   comp_off;    /* compressed-stream offset (key) */
    uint8_t   *data;        /* decompressed bytes (owned by this entry) */
    size_t     size;        /* number of decompressed bytes */
    size_t     last_user;   /* sorted-order index of last file using block */
    bc_entry_t *hash_next;  /* hash-chain */
};

typedef struct {
    bc_entry_t *buckets[BC_HASH_SIZE];
    size_t      count;
    size_t      total_bytes;  /* total decompressed bytes currently cached */
} bc_t;

static void bc_init(bc_t *c) { memset(c, 0, sizeof(*c)); }

static bc_entry_t *bc_lookup(bc_t *c, lzma_vli off) {
    for (bc_entry_t *e = c->buckets[BC_HASH(off)]; e; e = e->hash_next)
        if (e->comp_off == off) return e;
    return NULL;
}

/* Remove one entry from the cache (hash) and free its storage. */
static void bc_remove(bc_t *c, bc_entry_t *e) {
    bc_entry_t **pp = &c->buckets[BC_HASH(e->comp_off)];
    while (*pp && *pp != e) pp = &(*pp)->hash_next;
    if (*pp) *pp = e->hash_next;
    c->total_bytes -= e->size;
    free(e->data);
    free(e);
    --c->count;
}

/* Evict the entry whose *next* consumer is furthest ahead (true Bélády).
 * Uses lu_next_user (> current_idx, strictly) so that blocks whose last user
 * IS current_idx — but have already been served earlier in the current file's
 * lzma-block iteration — appear with next_use = SIZE_MAX and become the
 * preferred eviction target.  Using >= instead would give those dead blocks
 * next_use = current_idx (the smallest possible value), falsely marking them
 * as "hot" and causing a genuinely useful block to be evicted instead, which
 * produces a spurious re-decompression on that block's next access.
 *
 * A block still needed later in the current file's iteration (users include
 * current_idx AND some future index j) correctly shows next_use = j with >,
 * not current_idx with >=, so it is still protected from eviction when j is
 * closer than other candidates.  O(BC_MAX_ENTRIES) per call. */
static void bc_evict_belady(bc_t *c, const lu_table_t *lu, size_t current_idx) {
    bc_entry_t *victim = NULL;
    size_t victim_next = 0;
    for (int h = 0; h < BC_HASH_SIZE; ++h) {
        for (bc_entry_t *e = c->buckets[h]; e; e = e->hash_next) {
            size_t nxt = lu_next_user(lu, e->comp_off, current_idx);
            if (!victim || nxt > victim_next) {
                victim = e;
                victim_next = nxt;
            }
        }
    }
    if (victim) bc_remove(c, victim);
}

/* Insert a decompressed block into the cache, taking ownership of data.
 * Evicts as needed to stay within gBcMaxBytes (RAM limit) or gBcMaxEntries
 * (entry-count limit).  If both are zero/negative the cache is unbounded. */
static bc_entry_t *bc_insert(bc_t *c, lzma_vli off,
                              uint8_t *data, size_t size, size_t last_user,
                              const lu_table_t *lu, size_t current_idx) {
    if (gBcMaxBytes > 0) {
        /* RAM-based limit: evict until the new block fits, or until the cache
         * is empty.  If the block alone exceeds gBcMaxBytes we still insert it
         * (the data is needed) — the limit is best-effort in that edge case. */
        while (c->count > 0 && c->total_bytes + size > gBcMaxBytes)
            bc_evict_belady(c, lu, current_idx);
    } else if (gBcMaxEntries > 0) {
        /* Entry-count limit: evict one entry when the cap is reached. */
        if (c->count >= (size_t)gBcMaxEntries)
            bc_evict_belady(c, lu, current_idx);
    }
    /* gBcMaxEntries <= 0 and gBcMaxBytes == 0: unbounded — never evict. */
    bc_entry_t *e = xmalloc(sizeof(bc_entry_t));
    e->comp_off = off;
    e->data = data; e->size = size; e->last_user = last_user;
    e->hash_next = c->buckets[BC_HASH(off)];
    c->buckets[BC_HASH(off)] = e;
    ++c->count;
    c->total_bytes += size;
    return e;
}

/* Proactively drop all entries whose last consumer was file_idx. */
static void bc_evict_done(bc_t *c, size_t file_idx) {
    for (int h = 0; h < BC_HASH_SIZE; ++h) {
        bc_entry_t *e = c->buckets[h];
        while (e) {
            bc_entry_t *nx = e->hash_next;   /* save before possible free */
            if (e->last_user == file_idx) bc_remove(c, e);
            e = nx;
        }
    }
}

static void bc_free(bc_t *c) {
    for (int h = 0; h < BC_HASH_SIZE; ++h) {
        bc_entry_t *e = c->buckets[h];
        while (e) { bc_entry_t *nx = e->hash_next; free(e->data); free(e); e = nx; }
        c->buckets[h] = NULL;
    }
    c->count = 0;
}


/* ---- block decompression helper --------------------------------------- */

/* Decompress one lzma data block (located at comp_off in the input file)
 * into a freshly-allocated buffer.  The caller owns the returned buffer. */
static uint8_t *decompress_block_at(lzma_vli comp_off, lzma_check check,
                                    size_t outsize) {
    if (fseeko(gInFile, (off_t)comp_off, SEEK_SET) == -1)
        die("Error seeking to block");

    int hb = fgetc(gInFile);
    if (hb == EOF || hb == 0)
        die("Error reading block header byte");

    lzma_filter filters[LZMA_FILTERS_MAX + 1];
    lzma_block block = { .filters = filters, .check = check, .version = 0 };
    block.header_size = lzma_block_header_size_decode(hb);

    uint8_t hdrbuf[LZMA_BLOCK_HEADER_SIZE_MAX];
    hdrbuf[0] = (uint8_t)hb;
    if (fread(hdrbuf + 1, block.header_size - 1, 1, gInFile) != 1)
        die("Error reading block header");
    if (lzma_block_header_decode(&block, NULL, hdrbuf) != LZMA_OK)
        die("Error decoding block header");

    uint8_t *output = xmalloc(outsize);
    lzma_stream stream = LZMA_STREAM_INIT;
    if (lzma_block_decoder(&stream, &block) != LZMA_OK)
        die("Error creating block decoder");
    stream.next_out  = output;
    stream.avail_out = outsize;

    uint8_t ibuf[CHUNKSIZE];
    lzma_ret err = LZMA_OK;
    while (err != LZMA_STREAM_END) {
        if (err != LZMA_OK)
            die("Error decoding block data");
        if (stream.avail_in == 0) {
            stream.avail_in = fread(ibuf, 1, sizeof(ibuf), gInFile);
            if (ferror(gInFile))
                die("Error reading block data");
            stream.next_in = ibuf;
        }
        err = lzma_code(&stream, LZMA_RUN);
    }
    lzma_end(&stream);
    lzma_filters_free(filters, NULL);
    return output;
}


/* ---- streaming output helper ------------------------------------------ */

/* Write the bytes of the file range [fstart, fend) directly to gOutFile,
 * decompressing lzma blocks on demand.  Blocks with future consumers are
 * stored in the Bélády cache so they are decompressed only once; blocks
 * with no future consumer are used and freed immediately.
 *
 * If tail_zeros_out is non-NULL this function is additionally scanning for
 * the tar end-of-archive boundary while streaming.  It walks forward through
 * tar headers (each 512-byte header block followed by its rounded data) and
 * stops writing as soon as it finds an all-zero header (the EOF marker),
 * recording the number of trailing zeros in *tail_zeros_out.  This lets the
 * caller reproduce exactly the trailing-zero bytes at the end of the sorted
 * output without ever buffering the full file content in memory. */
static void stream_file_to_output(off_t fstart, off_t fend,
                                  lzma_vli fio, bc_t *cache, lu_table_t *lu,
                                  size_t current_idx, sort_stats_t *stats,
                                  size_t total_size, size_t *tail_zeros_out) {
    /* State for the optional forward tar-header scan. */
    off_t  eof_boundary  = fend;  /* updated once the EOF marker is found */
    off_t  next_hdr_pos  = 0;     /* file-relative offset of the next header */
    bool   eof_found     = false;

    lzma_index_iter iter;
    lzma_index_iter_init(&iter, gIndex);
    if (lzma_index_iter_locate(&iter, (lzma_vli)fstart))
        die("Can't locate block for uncompressed offset %jd",
            (intmax_t)fstart);

    do {
        if (iter.block.compressed_file_offset == fio)
            continue;

        off_t blk_ustart = (off_t)iter.block.uncompressed_file_offset;
        if (blk_ustart >= eof_boundary)
            break;
        off_t blk_uend = blk_ustart + (off_t)iter.block.uncompressed_size;

        bc_entry_t *ce = bc_lookup(cache, iter.block.compressed_file_offset);
        uint8_t *bdata_free = NULL;
        const uint8_t *bdata;
        if (ce) {
            stats->cache_hits++;
            stats->cache_hit_bytes += iter.block.uncompressed_size;
            bdata = ce->data;
        } else {
            if (!iter.stream.flags)
                die("Missing stream flags for block");
            if (lu_is_seen(lu, iter.block.compressed_file_offset)) {
                stats->redecompressions++;
                stats->redecompressed_bytes += iter.block.uncompressed_size;
            } else {
                stats->decompressions++;
                stats->decompressed_bytes += iter.block.uncompressed_size;
            }
            uint8_t *new_data = decompress_block_at(
                iter.block.compressed_file_offset,
                iter.stream.flags->check,
                (size_t)iter.block.uncompressed_size);
            if (lu_next_user(lu, iter.block.compressed_file_offset,
                             current_idx) != SIZE_MAX) {
                lu_mark_seen(lu, iter.block.compressed_file_offset);
                size_t lu_val = lu_last_user(lu,
                                    iter.block.compressed_file_offset);
                ce = bc_insert(cache,
                               iter.block.compressed_file_offset,
                               new_data, (size_t)iter.block.uncompressed_size,
                               lu_val, lu, current_idx);
                bdata = ce->data;
            } else {
                bdata = bdata_free = new_data;
            }
        }

        off_t cstart = fstart > blk_ustart ? fstart : blk_ustart;
        off_t cend   = eof_boundary < blk_uend ? eof_boundary : blk_uend;

        /* Forward tar-header scan: examine the bytes passing through this
         * block to detect the EOF marker, updating eof_boundary and
         * scan_file_pos as we go. */
        if (tail_zeros_out && !eof_found) {
            /* next_hdr_pos is file-relative; convert to absolute for
             * comparison with cstart/cend. */
            off_t abs_next = fstart + next_hdr_pos;
            while (!eof_found && abs_next + TAR_BLOCK_SIZE <= cend) {
                size_t boff = (size_t)(abs_next - blk_ustart);
                const uint8_t *hdr = bdata + boff;

                /* Check for all-zero header (EOF marker). */
                size_t k;
                for (k = 0; k < TAR_BLOCK_SIZE; k++)
                    if (hdr[k] != 0) break;
                if (k == TAR_BLOCK_SIZE) {
                    /* Found the EOF marker: stop writing here. */
                    eof_boundary = abs_next;
                    cend = (eof_boundary < blk_uend)
                           ? eof_boundary : blk_uend;
                    *tail_zeros_out = (size_t)total_size
                                      - (size_t)(eof_boundary - fstart);
                    eof_found = true;
                    break;
                }

                /* Parse the entry data size (octal or GNU base-256). */
                uint64_t entry_size = 0;
                if (hdr[124] & 0x80) {
                    for (k = 1; k < 12; k++)
                        entry_size = (entry_size << 8) | hdr[124 + k];
                } else {
                    for (k = 0; k < 12; k++) {
                        uint8_t c = hdr[124 + k];
                        if (c < '0' || c > '7') break;
                        entry_size = entry_size * 8 + (c - '0');
                    }
                }

                next_hdr_pos += TAR_BLOCK_SIZE
                                + (off_t)ROUND_UP_TO_TAR_BLOCK(entry_size);
                abs_next = fstart + next_hdr_pos;
            }
        }

        size_t len = (size_t)(cend - cstart);
        if (len > 0 && fwrite(bdata + (size_t)(cstart - blk_ustart),
                              len, 1, gOutFile) != 1)
            die("Error writing sorted output");
        free(bdata_free);
    } while (!lzma_index_iter_next(&iter, LZMA_INDEX_ITER_BLOCK));
}


/* ---- main entry point ------------------------------------------------- */

/* Load the pixz index once, sort the files, and write the tar entries
 * (headers + data) to gOutFile in that new order.
 *
 * Sort key: (is_header_only, is_large, extension, directory, name).
 *   is_header_only – dirs, symlinks, hard links, devices, FIFOs, and 0-byte
 *                files (span <= 512 bytes) sort first, and among themselves
 *                are ordered by filename only (no extension or size tiers).
 *                In a typical tar such entries immediately precede the files
 *                they contain, sharing a compressed block with them.  Sorting
 *                them first keeps them as early (not late) consumers of shared
 *                blocks, so lu_last_user is unaffected and cache pressure is
 *                minimised.
 *   is_large   – files >= threshold go last; they gain nothing from adjacency
 *                to other files and would disrupt runs of compressible content.
 *   extension  – groups globally similar types (all .c together, all .py
 *                together), giving the compressor a rich shared context per type.
 *   directory  – within the same type, same-directory files share
 *                project-specific identifiers the compressor exploits well.
 *   name       – stable tie-breaker.
 *
 * Bélády-optimal decompression cache: blocks shared by multiple files are
 * decompressed only once.  Blocks with no future consumer are decompressed,
 * copied, and freed immediately without entering the cache.  A per-block
 * access list (built in one prepass) drives both the proactive eviction
 * (bc_evict_done) and the Bélády fallback (bc_evict_belady): when the cache
 * is full, the entry whose *next* consumer is furthest ahead is evicted. */

static volatile sig_atomic_t sSortStatsPrint = 0;
static void sigusr1_handler(int sig) { (void)sig; sSortStatsPrint = 1; }

void pixz_sorted_extract(void) {
    struct sigaction sa, old_sa;
    sa.sa_handler = sigusr1_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    bool sigusr1_installed = (sigaction(SIGUSR1, &sa, &old_sa) == 0);

    if (!decode_index())
        die("Can't perform sorted extract on non-seekable input");

    lzma_vli fio = read_file_index();
    if (!fio)
        die("No file index found - not a tar archive");

    /* Count non-sentinel entries (sentinel has name == NULL). */
    size_t count = 0;
    for (file_index_t *f = gFileIndex; f && f->name; f = f->next)
        ++count;

    if (count == 0) {
        free_file_index();
        lzma_index_end(gIndex, NULL);
        sigaction(SIGUSR1, &old_sa, NULL);
        return;
    }

    /* Build a sorted array of file-index pointers. */
    file_index_t **sorted = xmalloc(count * sizeof(file_index_t *));
    size_t i = 0;
    for (file_index_t *f = gFileIndex; f && f->name; f = f->next)
        sorted[i++] = f;
    qsort(sorted, count, sizeof(file_index_t *), cmp_sorted_files);

    off_t  *starts = xmalloc(count * sizeof(off_t));
    size_t *sizes  = xmalloc(count * sizeof(size_t));
    for (i = 0; i < count; ++i) {
        starts[i] = sorted[i]->offset;
        sizes[i]  = (size_t)(sorted[i]->next->offset - sorted[i]->offset);
    }

    /* Find the position in the sorted array of the file that was last in the
     * original archive order.  Its raw size includes the original tar
     * end-of-archive zeros and must be trimmed before writing.
     * Use count as a sentinel meaning "not found". */
    size_t last_in_archive_sorted_idx = count;
    for (size_t j = 0; j < count; ++j) {
        if (sorted[j]->next->name == NULL) {
            last_in_archive_sorted_idx = j; break;
        }
    }

    /* Build the per-block access lists.
     *
     * Iterate sorted files in order (i = 0 … count-1).  For each file,
     * locate its overlapping blocks and record i as an accessing index.
     * Because i is non-decreasing, lu_add_access appends in sorted order.
     * After the loop, prune single-access entries: those blocks are never
     * shared and should never enter the cache. */
    lu_table_t lu;
    lu_init(&lu);
    for (i = 0; i < count; ++i) {
        off_t fend = starts[i] + (off_t)sizes[i];
        lzma_index_iter iter;
        lzma_index_iter_init(&iter, gIndex);
        if (lzma_index_iter_locate(&iter, (lzma_vli)starts[i]))
            continue;   /* offset out of range; skip (shouldn't happen) */
        do {
            if (iter.block.compressed_file_offset == fio)
                continue;
            if ((off_t)iter.block.uncompressed_file_offset >= fend)
                break;
            lu_add_access(&lu, iter.block.compressed_file_offset, i);
        } while (!lzma_index_iter_next(&iter, LZMA_INDEX_ITER_BLOCK));
    }
    lu_prune_single_user(&lu);

    /* Extract and write files in sorted order.
     *
     * For each file we stream directly to gOutFile via the Bélády block cache,
     * then evict any cache blocks whose last consumer was just this file. */
    bc_t   cache;
    bc_init(&cache);
    sort_stats_t stats = { 0, 0, 0, 0, 0, 0 };

    /* tail_zeros: the number of trailing zero bytes to write after all sorted
     * entries.  We reproduce exactly the bytes that were at the tail of the
     * original archive, whether that is the standard two-block EOF marker,
     * more (blocking-factor padding), or fewer (e.g. GNU tar without padding).
     * Defaults to 0 if last_in_archive_sorted_idx is not found (shouldn't
     * happen in practice). */
    size_t tail_zeros = 0;

    for (i = 0; i < count; ++i) {
        if (gVerbose && sorted[i]->name)
            fprintf(stderr, "%s\n", sorted[i]->name);

        /* For the last-in-archive entry, pass &tail_zeros so that
         * stream_file_to_output will run the forward tar-header scan inline
         * and stop writing at the EOF marker rather than padding. */
        size_t *tz = (i == last_in_archive_sorted_idx) ? &tail_zeros : NULL;
        stream_file_to_output(starts[i], starts[i] + (off_t)sizes[i],
                              fio, &cache, &lu, i, &stats, sizes[i], tz);

        /* Drop cache entries that no future file will need. */
        bc_evict_done(&cache, i);

        if (sSortStatsPrint) {
            /* Block SIGUSR1 while we clear the flag so that a signal
             * arriving between the check above and the clear below is not
             * silently lost (TOCTOU race). */
            sigset_t blk, old;
            sigemptyset(&blk);
            sigaddset(&blk, SIGUSR1);
            sigprocmask(SIG_BLOCK, &blk, &old);
            int do_print = sSortStatsPrint;
            if (do_print) sSortStatsPrint = 0;
            sigprocmask(SIG_SETMASK, &old, NULL);
            if (do_print)
                fprintf(stderr, "sorted-extract stats [%zu/%zu]:"
                        " decompressions=%zu (%.1f GiB)"
                        "  cache_hits=%zu (%.1f GiB)"
                        "  redecompressions=%zu (%.1f GiB)\n",
                        i + 1, count,
                        stats.decompressions,
                        (double)stats.decompressed_bytes / (1024.0*1024*1024),
                        stats.cache_hits,
                        (double)stats.cache_hit_bytes / (1024.0*1024*1024),
                        stats.redecompressions,
                        (double)stats.redecompressed_bytes / (1024.0*1024*1024));
        }
    }

    /* Write the trailing zero bytes from the original archive (may be fewer
     * than the standard 1024-byte EOF marker if the input was not padded). */
    if (tail_zeros > 0) {
        uint8_t *tar_eof = xmalloc(tail_zeros);
        memset(tar_eof, 0, tail_zeros);
        if (fwrite(tar_eof, tail_zeros, 1, gOutFile) != 1)
            die("Error writing tar EOF");
        free(tar_eof);
    }

    if (gVerbose)
        fprintf(stderr, "sorted-extract stats:"
                " decompressions=%zu (%.1f GiB)"
                "  cache_hits=%zu (%.1f GiB)"
                "  redecompressions=%zu (%.1f GiB)\n",
                stats.decompressions,
                (double)stats.decompressed_bytes / (1024.0*1024*1024),
                stats.cache_hits,
                (double)stats.cache_hit_bytes / (1024.0*1024*1024),
                stats.redecompressions,
                (double)stats.redecompressed_bytes / (1024.0*1024*1024));

    bc_free(&cache);
    lu_free(&lu);
    free(sorted);
    free(starts);
    free(sizes);
    free_file_index();
    lzma_index_end(gIndex, NULL);
    if (sigusr1_installed)
        sigaction(SIGUSR1, &old_sa, NULL);
}

