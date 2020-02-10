"""Given a file that looks a bit like a dataframe with rows that can be indexed
with some unique identifier, and another file with the same identifiers that
correspond to some additional key:values, glue the keyed values to the end of
the data using the identifiers.

I use <data> and <meta> rather loosely, but note that the many-to-one relationship
is one way. That is, the <meta> rows can be glued to multiple <data> rows, but
not the other way around.

Usage: bond <data> <meta>"""

import sys
import argparse

def main(args):
    if args.meta == '-':
        META_FH = sys.stdin
    else:
        META_FH = open(args.meta)

    meta = {}
    if args.mheader:
        meta_header_fields = args.mheader.split(",")
    else:
        meta_header_fields = META_FH.readline().strip().split(args.msep)
    if args.dropid:
        meta_header_fields.pop(args.mcol-1)

    if len(set(meta_header_fields)) != len(meta_header_fields):
        sys.stderr.write("Hey, listen! You have a duplicate key in your meta columns.\nThis will likely duplicate one column and suppress another.\n")

    field_index = list([i for i in range(len(meta_header_fields))])
    for line in META_FH:
        if len(line.strip()) == 0:
            print("Blank line."); sys.exit(1)
        fields = line.strip().split(args.msep)

        meta_id = fields[args.mcol-1]
        if args.dropid:
            fields.pop(args.mcol-1)

        meta[ meta_id ] = {
            meta_header_fields[i] : fields[i] for i in field_index
        }

    META_FH.close()


    if args.data == '-':
        DATA_FH = sys.stdin
    else:
        DATA_FH = open(args.data)

    if args.dheader:
        data_header_fields = args.dheader.split(",")
    else:
        data_header_fields = DATA_FH.readline().strip().split(args.dsep)


    append_names = []
    append_values = []
    if args.append:
        append_ = [x.split(":") for x in args.append.split(",")]
        append_names = [x[0] for x in append_]
        append_values = [x[1] for x in append_]

    print("\t".join(data_header_fields + meta_header_fields + append_names))
    for dline_i, line in enumerate(DATA_FH):
        fields = line.strip().split(args.dsep)
        data_id = fields[args.dcol - 1]

        best_key = None
        if args.greedy:
            key_sizes = [len(x) if data_id.startswith(x) else 0 for x in sorted(meta)]
            if sum(key_sizes) != 0:
                best_key = sorted(meta)[key_sizes.index(max(key_sizes))]
            metadata = [str(meta.get(best_key, {}).get(x, args.fill)) for x in meta_header_fields]
        else:
            metadata = [str(meta.get(data_id, {}).get(x, args.fill)) for x in meta_header_fields]

        if data_id not in meta and dline_i == 0 and not best_key:
            sys.stderr.write("First <data> row id was not found in <meta>, did you forget to specify --mheader for a headerless <meta>?")

        print('\t'.join(fields) + '\t' + "\t".join(metadata + append_values))


def cli():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("data")
    parser.add_argument("meta")
    parser.add_argument("--dheader", help="comma-delimited list of headers for input data, you must provide this if a header is missing")
    parser.add_argument("--mheader", help="comma-delimited list of headers for meta data, you must provide this if a header is missing")
    parser.add_argument("--dsep", help="data separator (default \\t)", default="\t")
    parser.add_argument("--msep", help="meta separator (default \\t)", default="\t")
    parser.add_argument("--dcol", help="data ID column number (1-based, default 1)", default=1, type=int)
    parser.add_argument("--mcol", help="meta ID column number (1-based, default 1)", default=1, type=int)
    parser.add_argument("--fill", help="value to fill meta cols if the ID does not appear in the meta (default -)", default="-")
    parser.add_argument("--dropid", help="drop the joining field from the metafile (default False)", action="store_true", default=False)
    parser.add_argument("--greedy", help="greedily match a meta ID to the start of the input data", action="store_true")
    parser.add_argument("--append", help="append some values to the end of every output row [format name1:value1,name2:value2,...]", default="")

    main(parser.parse_args())

if __name__ == "__main__":
    cli()
