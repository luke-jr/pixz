#!/bin/sh
# Test that -S extracts files sorted by file type (extension) then filename,
# and that header-only entries (directories, 0-byte files) sort before all
# regular files.
#
# Also verifies that a file whose content ends with zero bytes is not
# corrupted: the forward-parsing EOF-boundary detection must not confuse
# zero-valued file data with the tar end-of-archive marker.

PIXZ=../src/pixz

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Create a small tar archive with files of mixed extensions and names.
ALPHA_C=$TMPDIR/alpha.c
BETA_C=$TMPDIR/beta.c
MAIN_H=$TMPDIR/main.h
NOTES_TXT=$TMPDIR/notes.txt
README_TXT=$TMPDIR/readme.txt
MAKEFILE=$TMPDIR/Makefile

printf 'alpha\n'   > "$ALPHA_C"
printf 'beta\n'    > "$BETA_C"
printf 'main\n'    > "$MAIN_H"
printf 'notes\n'   > "$NOTES_TXT"
printf 'readme\n'  > "$README_TXT"
printf 'build\n'   > "$MAKEFILE"

# Add a sub-directory to verify that header-only entries sort first.
mkdir -p "$TMPDIR/subdir"
printf 'sub\n' > "$TMPDIR/subdir/sub.c"

# zeros.bin: a file whose content is entirely zero bytes.  Its last archive
# block therefore looks identical to a tar EOF block; the extractor must use
# the header size field rather than scanning backwards to find the boundary.
ZEROS_BIN=$TMPDIR/zeros.bin
dd if=/dev/zero of="$ZEROS_BIN" bs=512 count=4 2>/dev/null

TAR_FILE=$TMPDIR/test.tar
PIXZ_FILE=$TMPDIR/test.tpxz
SORTED_TAR=$TMPDIR/sorted.tar

# Build tar in an order that differs from the sorted result.
# Archive order: readme.txt, main.h, Makefile, alpha.c, notes.txt, beta.c,
#                subdir/ (directory entry), subdir/sub.c, zeros.bin
# zeros.bin is placed LAST so it is the final entry before the EOF marker.
tar cf "$TAR_FILE" -C "$TMPDIR" readme.txt main.h Makefile alpha.c notes.txt beta.c subdir zeros.bin

$PIXZ "$TAR_FILE" "$PIXZ_FILE"

$PIXZ -S "$PIXZ_FILE" > "$SORTED_TAR"

# List files in the sorted tar and collect their names in order.
ACTUAL=$(tar tf "$SORTED_TAR" 2>&1)

# Expected sort:
#   Tier 0 (header-only): subdir/           <- directory entry, no data
#   Tier 1 small files by extension then name:
#     no extension: Makefile
#     .bin:         zeros.bin
#     .c:           alpha.c, beta.c, subdir/sub.c
#     .h:           main.h
#     .txt:         notes.txt, readme.txt
EXPECTED="subdir/
Makefile
zeros.bin
alpha.c
beta.c
subdir/sub.c
main.h
notes.txt
readme.txt"

if [ "$ACTUAL" != "$EXPECTED" ]; then
    echo "FAIL: sorted order mismatch"
    echo "Expected:"
    echo "$EXPECTED"
    echo "Got:"
    echo "$ACTUAL"
    exit 1
fi

# Also verify that file contents survive the round-trip.
EXTRACT_DIR=$TMPDIR/extracted
mkdir -p "$EXTRACT_DIR"
tar xf "$SORTED_TAR" -C "$EXTRACT_DIR"

for name in alpha.c beta.c main.h notes.txt readme.txt Makefile subdir/sub.c zeros.bin; do
    ORIG_MD5=$(md5sum "$TMPDIR/$name" | awk '{print $1}')
    EXTR_MD5=$(md5sum "$EXTRACT_DIR/$name" | awk '{print $1}')
    if [ "$ORIG_MD5" != "$EXTR_MD5" ]; then
        echo "FAIL: content mismatch for $name"
        exit 1
    fi
done

# Verify that the sorted output is the same size as the original tar.
# GNU tar pads archives to a multiple of the blocking factor (default 10240
# bytes); that trailing padding must be preserved so the output size matches.
ORIG_SIZE=$(wc -c < "$TAR_FILE")
SORTED_SIZE=$(wc -c < "$SORTED_TAR")
if [ "$ORIG_SIZE" != "$SORTED_SIZE" ]; then
    echo "FAIL: size mismatch: original=$ORIG_SIZE sorted=$SORTED_SIZE"
    exit 1
fi

exit 0
