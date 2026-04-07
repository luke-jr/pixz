#!/bin/sh
# Test that -S extracts files sorted by file type (extension) then filename.

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

TAR_FILE=$TMPDIR/test.tar
PIXZ_FILE=$TMPDIR/test.tpxz
SORTED_TAR=$TMPDIR/sorted.tar

# Build tar in an order that differs from the sorted result.
# Archive order: readme.txt, main.h, Makefile, alpha.c, notes.txt, beta.c
tar cf "$TAR_FILE" -C "$TMPDIR" readme.txt main.h Makefile alpha.c notes.txt beta.c

$PIXZ "$TAR_FILE" "$PIXZ_FILE"

$PIXZ -S "$PIXZ_FILE" > "$SORTED_TAR"

# List files in the sorted tar and collect their names in order.
ACTUAL=$(tar tf "$SORTED_TAR" 2>&1)

# Expected sort: by extension (empty < .c < .h < .txt), then by name.
#   No extension: Makefile
#   .c:           alpha.c, beta.c
#   .h:           main.h
#   .txt:         notes.txt, readme.txt
EXPECTED="Makefile
alpha.c
beta.c
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

for name in alpha.c beta.c main.h notes.txt readme.txt Makefile; do
    ORIG=$(cat "$TMPDIR/$name")
    EXTR=$(cat "$EXTRACT_DIR/$name")
    if [ "$ORIG" != "$EXTR" ]; then
        echo "FAIL: content mismatch for $name"
        exit 1
    fi
done

exit 0
