#include "pixz.h"

#pragma mark FUNCTION DEFINITIONS

void pixz_list(bool tar) {
    if (!decode_index())
		die("Can't list non-seekable input");
	
    lzma_index_iter iter;
    lzma_index_iter_init(&iter, gIndex);

    if (tar && read_file_index()) {
        if (gVerbose) {
            lzma_index_iter viter;
            lzma_index_iter_init(&viter, gIndex);
            for (file_index_t *f = gFileIndex; f != NULL; f = f->next) {
                if (!f->name)
                    continue;
                if (lzma_index_iter_locate(&viter, (lzma_vli)f->offset))
                    die("Can't locate block for uncompressed offset %"PRIuMAX,
                        (uintmax_t)f->offset);
                lzma_vli uoffset_in_block = (lzma_vli)f->offset
                    - viter.block.uncompressed_file_offset;
                printf("%"PRIuMAX" %"PRIuMAX" %"PRIuMAX" %s\n",
                    (uintmax_t)viter.block.compressed_file_offset,
                    (uintmax_t)uoffset_in_block,
                    (uintmax_t)viter.block.uncompressed_size,
                    f->name);
            }
        } else {
            dump_file_index(stdout, false);
        }
        free_file_index();
    } else {
        while (!lzma_index_iter_next(&iter, LZMA_INDEX_ITER_BLOCK)) {
            printf("%9"PRIuMAX" / %9"PRIuMAX"\n",
                (uintmax_t)iter.block.unpadded_size,
                (uintmax_t)iter.block.uncompressed_size);
        }
    }
    
    lzma_index_end(gIndex, NULL);
    lzma_end(&gStream);
}
