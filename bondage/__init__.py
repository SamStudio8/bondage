"""Given a file that looks a bit like a dataframe with some unique identifier,
and another file with some key and values, add the keyed values to the frame
by the unique identifier. By default we'll assume the ID to join on, is the
first column in each file, and that both files have headers, and are tab delimited.

Usage: <data> <meta>"""

import sys
import argparse


def main(args):
    META_FH = open(args.meta)
    meta = {}
    if args.mheader:
        meta_header_fields = args.mheader.split(",")
    else:
        meta_header_fields = META_FH.readline().strip().split(args.msep)

    meta_offset = 0
    if args.dropid:
        meta_offset = 1

    for line in META_FH:
        if len(line.strip()) == 0:
            print("Blank line."); sys.exit(1)
        fields = line.strip().split(args.msep)
        meta[ fields[0] ] = {
            meta_header_fields[i] : fields[i] for i in range(meta_offset, len(meta_header_fields))
        }
    META_FH.close()


    DATA_FH = open(args.data)

    if args.dheader:
        data_header_fields = args.dheader.split(",")
    else:
        data_header_fields = DATA_FH.readline().strip().split(args.dsep)

    print("\t".join(data_header_fields + meta_header_fields[meta_offset:]))
    for line in DATA_FH:
        fields = line.strip().split(args.dsep)
        cid = fields[0]

        if args.greedy:
            key_sizes = [len(x) if fields[0].startswith(x) else 0 for x in sorted(meta)]

            if sum(key_sizes) == 0:
                best_key = None
            else:
                best_key = sorted(meta)[key_sizes.index(max(key_sizes))]
            metadata = [str(meta.get(best_key, {}).get(x, args.empty)) for x in meta_header_fields[meta_offset:]]
        else:
            metadata = [str(meta.get(fields[0], {}).get(x, args.empty)) for x in meta_header_fields[meta_offset:]]

        print('\t'.join(fields) + '\t' + "\t".join(metadata))


def cli():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("data")
    parser.add_argument("meta")
    parser.add_argument("--greedy", help="greedily match a meta ID to the start of the input data", action="store_true")
    parser.add_argument("--dheader", help="comma-delimited list of headers for input data, you must provide this if a header is missing")
    parser.add_argument("--mheader", help="comma-delimited list of headers for meta data, you must provide this if a header is missing")
    parser.add_argument("--dsep", help="data separator (default \\t)", default="\t")
    parser.add_argument("--msep", help="meta separator (default \\t)", default="\t")
    parser.add_argument("--empty", help="value to use if the ID does not appear in the metadata (default -)", default="-")
    parser.add_argument("--dropid", help="drop the joining field from the metafile (default False)", action="store_true", default=False)

    main(parser.parse_args())

if __name__ == "__main__":
    cli()
