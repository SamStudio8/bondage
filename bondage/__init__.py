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

def coerce_cols(l, labels):
    for t_i, t_col in enumerate(l):
        try:
            # Coerce column to 0-indexed int
            l[t_i] = int(t_col)-1
            continue
        except ValueError:
            # Otherwise it's a named column
            try:
                l[t_i] = labels.index(t_col)
                continue
            except ValueError:
                # Pass through to general Exception
                pass
        raise Exception("Could not coerce column '%d:%s' to a column in %s" % (t_i, t_col, labels))
    return l

def main(args):


    # Open data stream
    if args.data == '-':
        DATA_FH = sys.stdin
    else:
        DATA_FH = open(args.data)

    # Read data header (or parse the provided one)
    if args.dheader:
        data_header_fields = args.dheader.split(",")
    else:
        data_header_fields = DATA_FH.readline().strip().split(args.dsep)

    # Open meta stream
    if args.meta == '-':
        META_FH = sys.stdin
    else:
        META_FH = open(args.meta)

    # Read meta header (or parse the provided one)
    meta = {}
    if args.mheader:
        meta_header_fields = args.mheader.split(",")
    else:
        meta_header_fields = META_FH.readline().strip().split(args.msep)

    args.dcol = coerce_cols(args.dcol.split(','), data_header_fields)
    args.mcol = coerce_cols(args.mcol.split(','), meta_header_fields)

    # Drop repeated columns if desired
    if args.dropid:
        dropped = 0
        for mcol in sorted(args.mcol):
            meta_header_fields.pop(mcol-dropped)
            dropped += 1

    if len(set(meta_header_fields)) != len(meta_header_fields):
        sys.stderr.write("Hey, listen! You have a duplicate key in your meta columns.\nThis will likely duplicate one column and suppress another.\n")

    field_index = list([i for i in range(len(meta_header_fields))])
    for line in META_FH:
        if len(line.strip()) == 0:
            print("Blank line."); sys.exit(1)
        fields = line.strip().split(args.msep)

        meta_id = ":".join(fields[mcol] for mcol in args.mcol)
        if args.dropid:
            dropped = 0
            for mcol in sorted(args.mcol):
                fields.pop(mcol-dropped)
                dropped += 1

        meta[ meta_id ] = {
            meta_header_fields[i] : fields[i] for i in field_index
        }

    META_FH.close()

    append_names = []
    append_values = []
    if args.append:
        append_ = [x.split(":") for x in args.append.split(",")]
        append_names = [x[0] for x in append_]
        append_values = [x[1] for x in append_]

    print("\t".join(data_header_fields + meta_header_fields + append_names))
    for dline_i, line in enumerate(DATA_FH):
        fields = line.strip().split(args.dsep)
        data_id = ":".join(fields[dcol - 1] for dcol in args.dcol)

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
            if not args.force:
                raise Exception
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
    parser.add_argument("--dcol", help="data ID column numbers (1-based, default 1)", default='1')
    parser.add_argument("--mcol", help="meta ID column numbers (1-based, default 1)", default='1')
    parser.add_argument("--fill", help="value to fill meta cols if the ID does not appear in the meta (default -)", default="-")
    parser.add_argument("--dropid", help="drop the joining field(s) from the metafile (default False)", action="store_true", default=False)
    parser.add_argument("--greedy", help="greedily match a meta ID to the start of the input data (will only greedily match the first colum in a multicolumn bond)", action="store_true")
    parser.add_argument("--append", help="append some values to the end of every output row [format name1:value1,name2:value2,...]", default="")
    parser.add_argument("--force", help="Do whatever it takes to run this command successfully, even if it produces absolute garbage", action="store_true")

    main(parser.parse_args())

if __name__ == "__main__":
    cli()
