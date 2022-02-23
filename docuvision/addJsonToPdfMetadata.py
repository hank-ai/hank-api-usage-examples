#%%
#####
## Hank.ai Inc
# (c) 2022
# 
# Hank.ai DocuVision API Sample Code
#
# Add json response data to a corresponding pdf's metadata
# Q: Why the hell would you want to do this? 
# A: So you can keep the full docuvision extraction INSIDE the actual pdf so you don't have to keep up with the pdf and the .json file individually

#   If --json not passed, will convert ALL .json files in the given --dir to .csv format (or current dir if --dir not passed)
#   if --json is passed, will convert ONLY that .json file
#
# Example command line command to convert all jsons in directory c:/test/ and create seperate csvs for each unique pid
#   python addJsonToPdfMetadata.py --dir samples/ --loglevel DEBUG 
#
# Prerequisites:
# - Python 3.7.7 or greater installed
#   - pandas (pip install pandas)
#   - pypdf2 (pip install pypdf2)
# Args: (command line args)
# - --dir: (optional) full path to a directory on the current machine you'd like to post to DocuVision API. default=current directory
# - --recursive: (optional) if 1 and --dir passed and not --json passed, will look inside --dir recursively and process .json files  
# - --overwriteoriginal: (optional) if 1 will overwrite the original pdf. if 0 (default), will create a new pdf of filestem_dv.pdf 
# - --json: (optional) full path to a .json file result from DocuVision to convert to csv. If given, --dir will be ignored
# - --loglevel: (optional) how verbose you want to logging to be to the csvconversion.log file
# Results:
# - will embed the docuvision response into a pdf's metadata of the same filename.stem for easier lookup
# - the custom meta will have a key of 'docuvision_results' and the value will be a dumped json
# - will append logging info to jsontopdf.log in same dir as this script is run from


import os, logging, datetime, argparse, json, pprint
import pandas as pd
from pathlib import Path, PurePath
from PyPDF2 import PdfFileReader, PdfFileMerger, PdfFileWriter

#get command line parameters passed
ap = argparse.ArgumentParser("docuvision_json_to_csv_conversion_sample", epilog="For help contact support@hank.ai") #, exit_on_error=False)
ap.add_argument("--json", help="Path to a .json file to convert to csv (use this OR --dir)", 
    default=None, type=str)
ap.add_argument("--dir", help="Full path to a directory on the current machine to process .json files in (use this or --json)", 
    default=".", type=str)
ap.add_argument("--recursive", help="if 1 and --dir passed and not --json passed, will look inside --dir recursively and process .json files", 
    default=0, type=int)
ap.add_argument("--overwriteoriginal", help="if 1 will overwrite the original pdf. if 0 (default), will create a new pdf of filestem_dv.pdf", 
    default=0, type=int)
ap.add_argument("--loglevel", help="Logging level. Options are DEBUG, INFO, WARNING, ERROR, CRITICAL.", 
    default="DEBUG", type=str)

args, unknown = ap.parse_known_args()
if args.dir == '.': args.dir=os.path.dirname(os.path.realpath(__file__))
print("DOCUVISION SCRIPT STARTED")
print("args = ", args)

#setup logging
logging.basicConfig(
    filename='jsontopdf.log', 
    filemode='a', 
    level=args.loglevel.upper(),
    format='%(asctime)s:%(levelname)s:%(message)s')
logging.info("STARTING PROCESSING. {}".format(datetime.datetime.now()))
logging.info("Processing dir={} json={} recursive={}".format(args.dir, args.json, args.recursive))

if args.json is not None: 
    jsons = [args.json]
else: 
    dirP = Path(PurePath(Path.cwd(), args.dir))
    logging.debug(f"Dir to process {dirP}")
    if args.recursive:
        jsons = list(dirP.rglob("*.json"))
    else: 
        jsons = list(dirP.glob("*.json"))

#jsonP is a Path object pointing to the json filename to find a matching pdf to
#jsonobj is a json.load(...) object. will get dumped to the pdf custom metadata of the matching pdf
def addJsonToPdfMetadata(jsonP, jsonobj):
    try:
        pdfinP = jsonP.parent / (jsonP.stem[:jsonP.stem.rfind('_')] + '.pdf')
        pdfoutP = pdfinP.parent / (pdfinP.stem + '_dv.pdf')
        with open(pdfinP, 'rb') as fi:
            pdf_reader = PdfFileReader(fi)
            # pdfmeta = pdf_reader.getDocumentInfo()
            
            # pdf_out = PdfFileWriter()
            # pdf_out.appendPagesFromReader(pdf_reader)
            # pdf_out.addMetadata(pdfmeta)

            pdf_out = PdfFileMerger()
            pdf_out.append(fi)

            pdf_out.addMetadata(
                {'/docuvision_results': json.dumps(jsonobj)} #json.dumps([{'key1':'val1'}, {'key1': 'val2'}])}
            )
            with open(pdfoutP, 'wb') as fo:
                pdf_out.write(fo)
        if args.overwriteoriginal: 
            pdfoutP.replace(pdfinP)
            pdfoutP = pdfinP
        logging.debug(f"Wrote output pdf with json in metadata to {pdfoutP.name}")
    except Exception as e:
        logging.error(f"Error processing {jsonP.name}. {e}")

#loop through all the json files in the directory running the script
for j in jsons:
    logging.info(f"Processing {j.name}")
    print(f"Processing {j.name}")
    with open(j, 'r') as f:
        jstr = json.load(f) 
    addJsonToPdfMetadata(j, jstr)
    
logging.info("DONE")