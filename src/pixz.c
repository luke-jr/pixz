#include "pixz.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>

typedef enum {
    OP_WRITE,
    OP_READ,
    OP_EXTRACT,
    OP_LIST,
    OP_SORT_EXTRACT
} pixz_op_t;

static bool strsuf(char *big, char *small);
static char *subsuf(char *in, char *suf1, char *suf2);
static char *auto_output(pixz_op_t op, char *in);

static void usage(const char *msg) {
	if (msg)
		fprintf(stderr, "%s\n\n", msg);
	
	fprintf(stderr,
"pixz: Parallel Indexing XZ compression, fully compatible with XZ\n"
"\n"
"Basic usage:\n"
"  pixz input output.pxz           # Compress a file in parallel\n"
"  pixz -d input.pxz output        # Decompress\n"
"\n"
"Tarballs:\n"
"  pixz input.tar output.tpxz      # Compress and index a tarball\n"
"  pixz -d input.tpxz output.tar   # Decompress\n"
"  pixz -l input.tpxz              # List tarball contents very fast\n"
"  pixz -x path/to/file < input.tpxz | tar x  # Extract one file very fast\n"
"  pixz -S input.tpxz | tar x      # Extract sorted for best compression\n"
"  pixz -S -D 64M input.tpxz | pixz -9 > out.tpxz  # Recompress; classify >=64M as large\n"
"  tar -Ipixz -cf output.tpxz dir  # Make tar use pixz automatically\n"
"\n"
"Input and output:\n"
"  pixz < input > output.pxz       # Same as `pixz input output.pxz`\n"
"  pixz -i input -o output.pxz     # Ditto\n"
"  pixz [-d] input                 # Automatically choose output filename\n"
"\n"
"Other flags:\n"
"  -0, -1 ... -9      Set compression level, from fastest to strongest\n"
"  -p NUM             Use a maximum of NUM CPU-intensive threads\n"
"  -t                 Don't assume input is in tar format\n"
"  -k                 Keep original input (do not remove it)\n"
"  -c                 ignored\n"
"  -S                 Extract sorted by file type then filename (optimises compression)\n"
"  -S -l              Same, but list each file and print cache stats to stderr\n"
"                     Send SIGUSR1 to print running cache stats at any time during -S\n"
"  -D SIZE            Size threshold for classifying large files (used with -S);\n"
"                     files >= SIZE are sorted last; SIZE is in MiB when no suffix given;\n"
"                     K/M/G accepted (default: 8 MiB)\n"
"  -C LIMIT           Bélády block-cache limit for -S (default: 32 entries);\n"
"                     plain number = max cached blocks (-1 or 0 for unbounded);\n"
"                     with K/M/G suffix = max RAM for cached blocks (e.g. 512M)\n"
"  -V                 Print version and exit\n"
"  -h                 Print this help\n"
"\n"
"pixz %s\n"
"(C) 2009-2020 Dave Vasilevsky <dave@vasilevsky.ca>\n"
"https://github.com/vasi/pixz\n"
"You may use this software under the FreeBSD License\n",
	    PACKAGE_VERSION);
	if (msg)
		exit(2);
	exit(0);
}

static void version() {
    fprintf(stderr, "pixz %s\n", PACKAGE_VERSION);
    exit(0);
}

int main(int argc, char **argv) {    
    uint32_t level = LZMA_PRESET_DEFAULT;
    bool tar = true;
    bool keep_input = false;
    bool extreme = false;
    pixz_op_t op = OP_WRITE;
    char *ipath = NULL, *opath = NULL;
    
    int ch;
	char *optend;
	long optint;
    double optdbl;
    bool sort_dict_set = false;
    bool bc_limit_set = false;
    while ((ch = getopt(argc, argv, "dcxlSi:o:tkvVhp:0123456789f:q:eD:C:")) != -1) {
        switch (ch) {
            case 'c': break;
            case 'd': op = OP_READ; break;
            case 'x': op = OP_EXTRACT; break;
            case 'l':
                if (op == OP_SORT_EXTRACT)
                    gVerbose = true;
                else
                    op = OP_LIST;
                break;
            case 'S':
                if (op == OP_LIST)
                    gVerbose = true;
                op = OP_SORT_EXTRACT;
                break;
            case 'D': {
                char *end;
                unsigned long val = strtoul(optarg, &end, 10);
                if (end == optarg || val == 0)
                    usage("Need a positive integer argument to -D");
                unsigned long mult = 1024UL * 1024; /* bare number treated as MiB */
                if (*end == 'K' || *end == 'k') { mult = 1024UL; ++end; }
                else if (*end == 'M' || *end == 'm') { mult = 1024UL * 1024; ++end; }
                else if (*end == 'G' || *end == 'g') { mult = 1024UL * 1024 * 1024; ++end; }
                if (*end)
                    usage("Invalid suffix for -D; use K, M, or G");
                if (val > (unsigned long)(SIZE_MAX / mult))
                    usage("Value too large for -D");
                gSortDictSize = (size_t)(val * mult);
                sort_dict_set = true;
                break;
            }
            case 'C': {
                char *end;
                long cval = strtol(optarg, &end, 10);
                if (end == optarg)
                    usage("Need an integer argument to -C");
                if (*end == '\0') {
                    /* Plain integer: entry-count limit (-1 or 0 = unbounded). */
                    if (cval < -1)
                        usage("Invalid argument to -C; use -1 or 0 for unbounded");
                    gBcMaxEntries = (ssize_t)cval;
                    gBcMaxBytes   = 0;
                } else {
                    /* Suffixed value: RAM-based limit. */
                    if (cval <= 0)
                        usage("Need a positive integer for -C with a size suffix");
                    size_t mult;
                    if      (*end == 'K' || *end == 'k') { mult = 1024UL;                ++end; }
                    else if (*end == 'M' || *end == 'm') { mult = 1024UL * 1024;         ++end; }
                    else if (*end == 'G' || *end == 'g') { mult = 1024UL * 1024 * 1024; ++end; }
                    else usage("Invalid suffix for -C; use K, M, or G, or no suffix for entry count");
                    if (*end)
                        usage("Invalid suffix for -C; use K, M, or G");
                    if ((unsigned long)cval > (unsigned long)(SIZE_MAX / mult))
                        usage("Value too large for -C");
                    gBcMaxBytes   = (size_t)((unsigned long)cval * mult);
                    gBcMaxEntries = 0;
                }
                bc_limit_set = true;
                break;
            }
            case 'i': ipath = optarg; break;
            case 'o': opath = optarg; break;
            case 't': tar = false; break;
            case 'k': keep_input = true; break;
			case 'h': usage(NULL); break;
            case 'e': extreme = true; break;
            case 'V': version(); break;
			case 'f':
                optdbl = strtod(optarg, &optend);
                if (*optend || optdbl <= 0)
                    usage("Need a positive floating-point argument to -f");
                gBlockFraction = optdbl;
                break;
			case 'p':
				optint = strtol(optarg, &optend, 10);
				if (optint < 0 || *optend)
					usage("Need a non-negative integer argument to -p");
				gPipelineProcessMax = optint;
				break;
            case 'q':
    			optint = strtol(optarg, &optend, 10);
    			if (optint <= 0 || *optend)
    				usage("Need a positive integer argument to -q");
    			gPipelineQSize = optint;
    			break;
            default:
                if (ch >= '0' && ch <= '9') {
                    level = ch - '0';
                } else {
                    usage("");
                }
        }
    }
    argc -= optind;
    argv += optind;

    if (sort_dict_set && op != OP_SORT_EXTRACT)
        usage("-D is only meaningful with -S");
    if (bc_limit_set && op != OP_SORT_EXTRACT)
        usage("-C is only meaningful with -S");

    /* Default dict size: ask liblzma what LZMA_PRESET_DEFAULT uses. */
    if (!sort_dict_set) {
        lzma_options_lzma lzma_opt;
        if (lzma_lzma_preset(&lzma_opt, LZMA_PRESET_DEFAULT) == 0)
            gSortDictSize = lzma_opt.dict_size;
    }

    gInFile = stdin;
    gOutFile = stdout;
    bool iremove = false;    
    if (op != OP_EXTRACT && argc >= 1) {
        if (argc > 2 || (op == OP_LIST && argc == 2))
            usage("Too many arguments");
        if (ipath)
            usage("Multiple input files specified");
        ipath = argv[0];
        
        if (argc == 2) {
            if (opath)
                usage("Multiple output files specified");
            opath = argv[1];
        } else if (op != OP_LIST && op != OP_SORT_EXTRACT) {
            iremove = true;
            opath = auto_output(op, argv[0]);
			if (!opath)
				usage("Unknown suffix");
        }
    }

    if (ipath && !(gInFile = fopen(ipath, "r")))
      die("can not open input file: %s: %s", ipath, strerror(errno));

    if (opath) {
      if (gInFile == stdin) {
        // can't read permissions of original file, because we read from stdin,
        // using umask permissions
        if (!(gOutFile = fopen(opath, "w")))
          die("can not open output file: %s: %s", opath, strerror(errno));

      } else {
        // read permissions of original file,
        // use them to create / open output file
        struct stat input_stat;
        int output_fd;

        stat(ipath, &input_stat);

        if ((output_fd = open(opath, O_CREAT | O_WRONLY, input_stat.st_mode)) == -1)
          die("can not open output file: %s: %s", opath, strerror(errno));

        if (!(gOutFile = fdopen(output_fd, "w")))
          die("can not open output file: %s: %s", opath, strerror(errno));
      }
    }

#ifdef HAVE__SETMODE
    // Set files to binary encoding
    _setmode(_fileno(gInFile), O_BINARY);
    _setmode(_fileno(gOutFile), O_BINARY);
#endif

    switch (op) {
        case OP_WRITE:
			if (isatty(fileno(gOutFile)))
				usage("Refusing to output to a TTY");
			if (extreme)
				level |= LZMA_PRESET_EXTREME;
			pixz_write(tar, level);
			break;
        case OP_READ: pixz_read(tar, 0, NULL); break;
        case OP_EXTRACT: pixz_read(tar, argc, argv); break;
        case OP_LIST: pixz_list(tar); break;
        case OP_SORT_EXTRACT: pixz_sorted_extract(); break;
    }
    
    if (iremove && !keep_input)
        unlink(ipath);
    
    return 0;
}

#define SUF(_op, _s1, _s2) ({ \
    if (op == OP_##_op) { \
        char *r = subsuf(in, _s1, _s2); \
        if (r) \
            return r; \
    } \
})

static char *auto_output(pixz_op_t op, char *in) {
    SUF(READ, ".tar.xz", ".tar");
    SUF(READ, ".tpxz", ".tar");
    SUF(READ, ".xz", "");
    SUF(WRITE, ".tar", ".tpxz");
    SUF(WRITE, "", ".xz");
    return NULL;
}

static bool strsuf(char *big, char *small) {
    size_t bl = strlen(big), sl = strlen(small);
    return strcmp(big + bl - sl, small) == 0;
}

static char *subsuf(char *in, char *suf1, char *suf2) {
    if (!strsuf(in, suf1))
        return NULL;
    
    size_t li = strlen(in), l1 = strlen(suf1), l2 = strlen(suf2);
    char *r = xmalloc(li + l2 - l1 + 1);
    memcpy(r, in, li - l1);
    strcpy(r + li - l1, suf2);
    return r;
}
