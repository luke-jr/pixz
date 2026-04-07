#include "pixz.h"

#include <archive.h>
#include <archive_entry.h>


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

/* Return the file extension (including the dot) from the last path component.
 * Returns an empty string if there is no extension. */
static const char *file_type_ext(const char *name) {
    if (!name) return "";
    const char *slash = strrchr(name, '/');
    const char *base  = slash ? slash + 1 : name;
    const char *dot   = strrchr(base, '.');
    return dot ? dot : "";
}

static int cmp_sorted_files(const void *a, const void *b) {
    const file_index_t * const *fa = (const file_index_t * const *)a;
    const file_index_t * const *fb = (const file_index_t * const *)b;
    const char *na = (*fa)->name ? (*fa)->name : "";
    const char *nb = (*fb)->name ? (*fb)->name : "";

    /* Compare directory parts first: files that share a directory are most
     * similar to each other, so grouping them maximises LZMA compression
     * when the output is re-compressed. */
    const char *la = strrchr(na, '/');
    const char *lb = strrchr(nb, '/');
    size_t da = la ? (size_t)(la - na + 1) : 0;  /* length incl. trailing '/' */
    size_t db = lb ? (size_t)(lb - nb + 1) : 0;
    size_t dmin = da < db ? da : db;
    int r = strncmp(na, nb, dmin);
    if (r != 0) return r;
    if (da != db) return (da < db) ? -1 : 1;

    /* Same directory: group by extension so similar file types are adjacent. */
    r = strcmp(file_type_ext(na), file_type_ext(nb));
    if (r != 0) return r;

    /* Same directory + extension: stable order by full path. */
    return strcmp(na, nb);
}


/* ---- last-user lookup table -------------------------------------------
 *
 * Maps lzma block compressed-offset → the highest sorted-order file index
 * that overlaps that block.  Used to decide when a cached block can be
 * proactively evicted.
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
    size_t     last_user;   /* sorted-order index of last file using block */
    lu_entry_t *next;       /* hash-chain */
};

typedef struct { lu_entry_t *b[LU_HASH_SIZE]; } lu_table_t;

static void lu_init(lu_table_t *t) { memset(t, 0, sizeof(*t)); }

/* Always overwrites; caller guarantees user values are non-decreasing. */
static void lu_set(lu_table_t *t, lzma_vli off, size_t user) {
    size_t h = LU_HASH(off);
    for (lu_entry_t *e = t->b[h]; e; e = e->next) {
        if (e->comp_off == off) { e->last_user = user; return; }
    }
    lu_entry_t *e = xmalloc(sizeof(lu_entry_t));
    e->comp_off = off; e->last_user = user;
    e->next = t->b[h]; t->b[h] = e;
}

/* Returns SIZE_MAX when the offset is not in the table. */
static size_t lu_get(const lu_table_t *t, lzma_vli off) {
    size_t h = LU_HASH(off);
    for (const lu_entry_t *e = t->b[h]; e; e = e->next)
        if (e->comp_off == off) return e->last_user;
    return SIZE_MAX;
}

static void lu_free(lu_table_t *t) {
    for (int h = 0; h < LU_HASH_SIZE; ++h) {
        lu_entry_t *e = t->b[h];
        while (e) { lu_entry_t *n = e->next; free(e); e = n; }
        t->b[h] = NULL;
    }
}


/* ---- Bélády-optimal block cache ---------------------------------------
 *
 * Caches decompressed lzma blocks so that a block shared by several files
 * (in sorted order) is decompressed only once.  When the last consumer of
 * a block has been served, bc_evict_done() drops it immediately.  When the
 * cache is full and a new block must be inserted, bc_evict_belady() evicts
 * the entry whose last consumer is furthest ahead in sorted order — the
 * optimal (Bélády) policy.  Because bc_evict_done() guarantees that every
 * cached entry's last_user is strictly greater than the current file index,
 * the largest last_user value identifies the entry that can be deferred the
 * longest, making it the correct eviction target.
 */

#define BC_HASH_BITS    6
#define BC_HASH_SIZE    (1 << BC_HASH_BITS)
#define BC_HASH_MASK    (BC_HASH_SIZE - 1)
#define BC_MAX_ENTRIES  32          /* maximum number of cached blocks */
#define BC_HASH(off)    ((size_t)(off) & BC_HASH_MASK)

typedef struct bc_entry_t bc_entry_t;
struct bc_entry_t {
    lzma_vli   comp_off;    /* compressed-stream offset (key) */
    off_t      ustart;      /* uncompressed start of this block's data */
    uint8_t   *data;        /* decompressed bytes (owned by this entry) */
    size_t     size;        /* number of decompressed bytes */
    size_t     last_user;   /* sorted-order index of last file using block */
    bc_entry_t *hash_next;  /* hash-chain */
};

typedef struct {
    bc_entry_t *buckets[BC_HASH_SIZE];
    size_t      count;
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
    free(e->data);
    free(e);
    --c->count;
}

/* Evict the entry whose last consumer is furthest ahead (Bélády's policy).
 * bc_evict_done() guarantees that all cached entries have last_user strictly
 * greater than the current file index, so the entry with the largest
 * last_user is the correct eviction target. */
static void bc_evict_belady(bc_t *c) {
    bc_entry_t *victim = NULL;
    for (int h = 0; h < BC_HASH_SIZE; ++h) {
        for (bc_entry_t *e = c->buckets[h]; e; e = e->hash_next) {
            if (!victim || e->last_user > victim->last_user)
                victim = e;
        }
    }
    if (victim) bc_remove(c, victim);
}

/* Insert a decompressed block, taking ownership of data. */
static bc_entry_t *bc_insert(bc_t *c, lzma_vli off, off_t ustart,
                              uint8_t *data, size_t size, size_t last_user) {
    if (c->count >= BC_MAX_ENTRIES) bc_evict_belady(c);
    bc_entry_t *e = xmalloc(sizeof(bc_entry_t));
    e->comp_off = off; e->ustart = ustart;
    e->data = data; e->size = size; e->last_user = last_user;
    e->hash_next = c->buckets[BC_HASH(off)];
    c->buckets[BC_HASH(off)] = e;
    ++c->count;
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
    return output;
}


/* ---- per-file buffer fill --------------------------------------------- */

/* Copy the bytes of one file [fstart, fend) from lzma blocks into buf.
 * Blocks are fetched from (or added to) the LRU cache so that blocks shared
 * by consecutive sorted files are decompressed only once. */
static void fill_file_buf(uint8_t *buf, off_t fstart, off_t fend,
                          lzma_vli fio, bc_t *cache, const lu_table_t *lu) {
    lzma_index_iter iter;
    lzma_index_iter_init(&iter, gIndex);
    if (lzma_index_iter_locate(&iter, (lzma_vli)fstart))
        die("Can't locate block for uncompressed offset %jd",
            (intmax_t)fstart);

    do {
        if (iter.block.compressed_file_offset == fio)
            continue;   /* skip the pixz file-index block */

        off_t blk_ustart = (off_t)iter.block.uncompressed_file_offset;
        if (blk_ustart >= fend)
            break;      /* no more blocks overlap this file */
        off_t blk_uend = blk_ustart + (off_t)iter.block.uncompressed_size;

        /* Get the block from the cache, or decompress and cache it. */
        bc_entry_t *ce = bc_lookup(cache, iter.block.compressed_file_offset);
        if (!ce) {
            if (!iter.stream.flags)
                die("Missing stream flags for block");
            uint8_t *bdata = decompress_block_at(
                iter.block.compressed_file_offset,
                iter.stream.flags->check,
                (size_t)iter.block.uncompressed_size);
            size_t lu_val = lu_get(lu, iter.block.compressed_file_offset);
            ce = bc_insert(cache,
                           iter.block.compressed_file_offset, blk_ustart,
                           bdata, (size_t)iter.block.uncompressed_size,
                           lu_val);
        }

        /* Copy the overlapping byte range into the file buffer. */
        off_t cstart = fstart > blk_ustart ? fstart : blk_ustart;
        off_t cend   = fend   < blk_uend   ? fend   : blk_uend;
        memcpy(buf    + (size_t)(cstart - fstart),
               ce->data + (size_t)(cstart - blk_ustart),
               (size_t)(cend - cstart));
    } while (!lzma_index_iter_next(&iter, LZMA_INDEX_ITER_BLOCK));
}


/* ---- main entry point ------------------------------------------------- */

/* Load the pixz index once, sort it by directory then file type (extension)
 * then by filename within each directory, and write the tar entries (headers
 * + data) to gOutFile in that new order.  Grouping files by directory first
 * maximises LZMA compression when the output is re-compressed with pixz,
 * because files in the same directory share vocabulary (path prefixes,
 * identifiers, patterns) that fits well within LZMA's sliding-window
 * dictionary.  Within each directory, files are further grouped by extension.
 *
 * A Bélády-optimal decompression cache prevents repeated decompression of
 * lzma blocks that are shared by multiple files.  A last-user table (built
 * in one prepass over the sorted file list) drives both the proactive
 * eviction (bc_evict_done, called after each file) and the Bélády fallback
 * (bc_evict_belady): when the cache is full, the entry whose last consumer
 * is furthest ahead is evicted — the theoretically optimal choice. */
void pixz_sorted_extract(void) {
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

    /* Precompute last_user for every lzma block.
     *
     * Iterate sorted files in order (i = 0 … count-1).  For each file,
     * locate its overlapping blocks and call lu_set(..., i).  Because i
     * increases monotonically, each lu_set call overwrites with a larger
     * value, so at the end lu_get(block) == max sorted index that needs it. */
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
            lu_set(&lu, iter.block.compressed_file_offset, i);
        } while (!lzma_index_iter_next(&iter, LZMA_INDEX_ITER_BLOCK));
    }

    /* Extract and write files in sorted order.
     *
     * For each file we fill a temporary buffer from the LRU block cache,
     * write it to gOutFile, then evict any cache blocks whose last consumer
     * was just this file. */
    bc_t   cache;
    bc_init(&cache);
    uint8_t *fbuf     = NULL;
    size_t   fbuf_cap = 0;

    for (i = 0; i < count; ++i) {
        /* Grow the per-file working buffer on demand. */
        if (sizes[i] > fbuf_cap) {
            free(fbuf);
            fbuf     = xmalloc(sizes[i]);
            fbuf_cap = sizes[i];
        }

        fill_file_buf(fbuf, starts[i], starts[i] + (off_t)sizes[i],
                      fio, &cache, &lu);

        /* The entry that is last in archive order carries trailing tar
         * end-of-archive zeros in its size.  Trim those zeros so they
         * do not appear in the middle of the sorted output stream.
         * Scan backwards for the last non-zero byte, then round up to
         * the next TAR_BLOCK_SIZE boundary. */
        size_t write_size = sizes[i];
        if (i == last_in_archive_sorted_idx) {
            ssize_t last_nz = (ssize_t)write_size - 1;
            while (last_nz >= 0 && fbuf[last_nz] == 0)
                --last_nz;
            write_size = last_nz < 0 ? 0
                : ROUND_UP_TO_TAR_BLOCK((size_t)last_nz + 1);
        }

        if (write_size > 0 && fwrite(fbuf, write_size, 1, gOutFile) != 1)
            die("Error writing sorted output");

        /* Drop cache entries that no future file will need. */
        bc_evict_done(&cache, i);
    }

    /* Write the tar end-of-archive marker: two TAR_BLOCK_SIZE zero blocks. */
    uint8_t tar_eof[TAR_BLOCK_SIZE * 2];
    memset(tar_eof, 0, sizeof(tar_eof));
    if (fwrite(tar_eof, sizeof(tar_eof), 1, gOutFile) != 1)
        die("Error writing tar EOF");

    bc_free(&cache);
    lu_free(&lu);
    free(fbuf);
    free(sorted);
    free(starts);
    free(sizes);
    free_file_index();
    lzma_index_end(gIndex, NULL);
}

