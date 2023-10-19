import os
import requests
import astropy.io.fits as aIoFits
import astropy.table as aTable
import multiprocessing as mp


def _do_download(fname, tile, band, epoch, path, outdir):
    path = "/".join([
        path,
        "e%03d"%int(epoch),
        tile[:3],
        tile,
        fname,
    ])
    
    r = requests.get(path)
    if r.status_code != 200:
        raise Exception(
            "Status code {} received for \"{}\"".format(status_code, path)
        )
    
    open(os.path.join(outdir,fname), "wb").write(r.content)


def do_download(tile, band, epoch, path, outdir):
    fname = "unwise-%s-w%d-img-m.fits"%(tile,band)
    print("Downloading",fname)
    outdir = os.path.join(outdir, "e%03d"%int(epoch))
    try:
        os.makedirs(outdir)
    except FileExistsError:
        pass
    _do_download(fname, tile, band, epoch, path, outdir)

    # Download other data products as needed here...
    # f/e
    # fname = "unwise-%s-w%d-img-invvar-m.fits.gz"%(tile,band)
    # print("Downloading",fname)
    #_do_download(fname, tile, band, epoch, path, outdir)


def work(args):
    try:
        do_download(*args)
    except Exception as e:
        print(e)
        raise e


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("outdir", help="Where to save files")
    ap.add_argument("tile", help="What tile to download (e.g., 1612p590)")
    ap.add_argument(
        "--unwise-path",
        type=str,
        default="https://portal.nersc.gov/project/cosmo/temp/ameisner/neo8"
    )
    ap.add_argument("--index", type=str, default=None)
    ap.add_argument(
        "--n-workers",
        type=int,
        default=1,
        help="Number of concurrent workers"
    )
    args = ap.parse_args()

    index = args.index
    if index is None:
        idx_name = "tr_{}_index.fits".format(args.unwise_path.split("/")[-1])
        index = os.path.join(args.outdir, idx_name)
        if not os.path.exists(index):
            print("Index not found at \"{}\"".format(os.path.join(index)))
            print("Trying to download and save index file")
            idx_path = os.path.join(args.unwise_path, idx_name)
            r = requests.get(idx_path)
            if r.status_code != 200:
                raise Exception("Unable to download \"{}\"".format(idx_path))
            open(index,"wb").write(r.content)
            print("Saved index to",index)
    elif not (os.path.exists(index) and os.path.isfile(index)):
        raise Exception("Index file not found at \"{}\"".format(index))
    
    index = aTable.Table(aIoFits.open(index)[1].data).to_pandas()

    if not os.path.exists(args.outdir):
        os.mkdirs(args.outdir)

    if not os.path.isdir(args.outdir):
        raise Exception("\"{}\" is not a directory".format(args.outdir))

    index = index[index["COADD_ID"] == args.tile]
    if len(index) == 0:
        raise Exception("Invalid tile/coadd ID \"{}\"".format(args.tile))
    
    jobs = (
        (i["COADD_ID"], i["BAND"], i["EPOCH"], args.unwise_path, args.outdir)
        for _,i in index.iterrows()
    )
    if args.n_workers > 1:
        p = mp.Pool(args.n_workers)
        p.map(work,jobs)
    else:
        for job in jobs: work(job)


if __name__ == "__main__": main()
