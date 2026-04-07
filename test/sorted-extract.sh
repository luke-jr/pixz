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

# ---------------------------------------------------------------------------
# Test 2: truncated archive — last entry is a 0-byte file and the archive
# ends with only a handful of null bytes (no proper 1024-byte EOF marker).
# This is non-conformant but pixz must reproduce the exact tail faithfully.
# ---------------------------------------------------------------------------

TRUNC_DIR=$TMPDIR/trunc
mkdir -p "$TRUNC_DIR"
printf 'data\n' > "$TRUNC_DIR/data.txt"
> "$TRUNC_DIR/empty"           # 0-byte file; this will be the last entry

# Build a normal tar, then truncate it to simulate a stripped EOF.
TRUNC_TAR=$TMPDIR/trunc.tar
tar cf "$TRUNC_TAR" -C "$TRUNC_DIR" data.txt empty

# Find where the archive content actually ends (strip the standard 1024-byte
# EOF marker so only a small stub remains).
TRUNC_SIZE=$(wc -c < "$TRUNC_TAR")
# Keep all but 1020 bytes of the trailing zeros (leave 4 null bytes).
CONTENT_SIZE=$((TRUNC_SIZE - 1020))
dd if="$TRUNC_TAR" of="$TRUNC_TAR.short" bs=1 count="$CONTENT_SIZE" 2>/dev/null
mv "$TRUNC_TAR.short" "$TRUNC_TAR"

TRUNC_PIXZ=$TMPDIR/trunc.tpxz
TRUNC_SORTED=$TMPDIR/trunc_sorted.tar

LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" $PIXZ "$TRUNC_TAR" "$TRUNC_PIXZ" 2>/dev/null || true
$PIXZ -S "$TRUNC_PIXZ" > "$TRUNC_SORTED"

# Output must be exactly the same byte count as the truncated input.
TRUNC_ORIG_SIZE=$(wc -c < "$TRUNC_TAR")
TRUNC_SORTED_SIZE=$(wc -c < "$TRUNC_SORTED")
if [ "$TRUNC_ORIG_SIZE" != "$TRUNC_SORTED_SIZE" ]; then
    echo "FAIL (truncated archive): size mismatch: original=$TRUNC_ORIG_SIZE sorted=$TRUNC_SORTED_SIZE"
    exit 1
fi

# The 0-byte file must still be present in the sorted output.
TRUNC_ACTUAL=$(tar tf "$TRUNC_SORTED" 2>/dev/null || true)
case "$TRUNC_ACTUAL" in
    *empty*) ;;
    *)
        echo "FAIL (truncated archive): 'empty' not found in sorted output"
        echo "Got: $TRUNC_ACTUAL"
        exit 1
        ;;
esac

exit 0
