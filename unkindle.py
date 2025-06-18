#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from io import BytesIO
import os
from pathlib import Path
import re
import requests
import shutil
import xml.etree.ElementTree as ET
from joblib import Parallel, delayed

from third_party.ion import DrmIon,DrmIonVoucher
from third_party.kfxlib.yj_book import YJ_Book
from third_party.kfxlib import windows_long_path_fix, file_write_binary
from yj_to_folder import KFX_CBZ

import amazon_api
import config


class Kindle:
    def __init__(self, asin, ignore_hdv = False, hdv_png = True):
        self.asin = asin
        self.session = requests.Session()
        self.ignore_hdv = ignore_hdv
        self.hdv_png = hdv_png

        print("Authenticating . . .")
        self.auth_state = amazon_api.login(None, None, config.DOMAIN)
        if not self.auth_state:
            raise Exception("Could not authenticate!")
        print("Authenticated!")

    def decrypt_voucher(self, voucher_data):
        with BytesIO(voucher_data) as voucher_data_io:
            voucher = DrmIonVoucher(voucher_data_io, self.auth_state["device"]["device_id"], "")
            voucher.parse()
            voucher.decryptvoucher()
        return voucher


    def decrypt_kfx(self, kfx_data):
        if kfx_data[:8] != b'\xeaDRMION\xee':
            return kfx_data

        with BytesIO() as decrypted_data:
            DrmIon(BytesIO(kfx_data[8:-8]), lambda name: self.drm_voucher).parse(decrypted_data)
            return decrypted_data.getvalue()


    # TODO: async
    def get_resource(self, resource):
        resp = self.session.send(
            amazon_api.signed_request(
                "GET",
                resource["endpoint"]["url"],
                asin = self.asin,
                state = self.auth_state,
                request_id = resource["id"],
                request_type = resource["type"]
            )
        )

        filename = resource["id"]
        if resource["type"] == "DRM_VOUCHER":
            filename += ".ast"
        elif resource["type"] == "KINDLE_MAIN_METADATA" or resource["type"] == "KINDLE_MAIN_BASE" or resource["type"] == "KINDLE_MAIN_ATTACHABLE":
            filename += ".kfx"

        return (resp.content, filename)


    def get_book(self):
        manifest_resp = self.session.send(
            amazon_api.signed_request(
                "GET", 
                config.API_MANIFEST_URL + self.asin + config.API_VOUCHER_VERSIONS,
                asin = self.asin,
                state = self.auth_state,
                request_type="manifest"
            )
        )
        
        if manifest_resp.status_code != 200:
            print(manifest_resp.text)
            print("Could not acquire the manifest!")
            return False
        
        self.resources = manifest_resp.json()["resources"]

        drm_voucher = [resource for resource in self.resources if resource["type"] == "DRM_VOUCHER"][0]
        drm_voucher, drm_voucher_filename = self.get_resource(drm_voucher)

        try:
            self.drm_voucher = self.decrypt_voucher(drm_voucher)
        except:
            print("Could not decrypt the drm voucher!")
            return False
        
        # TODO: get metadata here
        # decoding it with kfxlib is a bit tricky, and for some books for some reason decoding
        # it before everything else is acquired makes it impossible to decode the rest of the book
        # could also get metadata from an api

        total = 0
        for resource in [resource for resource in self.resources if "size" in resource]:
            total += resource["size"]
        print(f"Download size: {int(total/1024/1024)}MB")

        return True

    def download_resource(self, res):
        print(f"Downloading {res['id']}")
        data, filename = self.get_resource(res)        
        with open(os.path.join(self.temp_folder, filename), "wb") as f:
            if filename.endswith(".kfx"):
                data = self.decrypt_kfx(data)
            f.write(data)    
        print(f"Saved {filename}")

    def download(self, output_folder = None, keep_temp = False):
        if not output_folder:
            output_folder = os.path.join(os.getcwd(), "output")
        self.output_folder = output_folder
        self.temp_folder = os.path.join(output_folder, self.asin)

        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder)

        Path(self.output_folder).mkdir(exist_ok=True)
        Path(self.temp_folder).mkdir(exist_ok=True)

        # TODO: parallel downloads
        result = Parallel(n_jobs=4, prefer="threads")(delayed(self.download_resource)(res) for res in self.resources)

        # #download in order
        # i = 0
        # for resource in self.resources[i:]:
        #     print(f"Downloading the book . . . {int(i/len(self.resources)*100)}%\r", end="")
        #     self.download_resources(resource)
        #     i += 1

        print("\nExtracting . . . ")        
              
        self.book = YJ_Book(self.temp_folder)

        # group_name = 'group'
        # !!!!!DO NOT USE self.book.get_metadata()!!!!!
        self.book.decode_book()
        metadata = self.book.get_yj_metadata_from_book()
        
        
        # if "Vol." in metadata.title:
            # title, vol = metadata.title.split("Vol.")
            # vol = re.findall(r'\d+', vol)[0]
            # self.release_name = f"{title.strip()} v{vol.strip().zfill(2)} "
        # elif " #" in metadata.title:
            # title, issue = metadata.title.split(" #")
            # issue = re.findall(r'\d+', issue)[0]
            # self.release_name = f"{title.strip()} {issue.strip().zfill(3)} "
        # else:
            # self.release_name = metadata.title

        # """
        # # TODO: get release year
        # if True:#not metadata.issue_date:
            # self.release_name += f"(xxxx) (digital) ({group_name})"
        # """

        # sanitize file name
        self.release_name = metadata.title
        self.release_name = re.sub(r"[\\/*?:\"'<>|]", "", self.release_name).strip()
        
        self.authur = metadata.authors[0]
        self.authur = re.sub(r"[\\/*?:\"'<>|]", "", self.authur).strip()
        print(f"{self.asin} : [{self.authur}] {self.release_name}")
        
        # --------------del by cunhan, do not output pictures.-----------------
        # content_folder = os.path.join(self.output_folder, self.release_name)
        # Path(content_folder).mkdir(exist_ok=True)
        
        # self.book.extract_to_folder(content_folder, f"{self.release_name} - p")


        # # ------------add by cunhan----------        
        print("Saving kfx")
        file_write_binary(os.path.join(self.output_folder, f"[{self.authur}] {self.release_name}.kfx"), self.book.convert_to_single_kfx())
        print("Saving epub")
        file_write_binary(os.path.join(self.output_folder, f"[{self.authur}] {self.release_name}.epub"), self.book.convert_to_epub())
            
        if not keep_temp:
            shutil.rmtree(self.temp_folder)
    
    def get_book_list(self, n, email):
        '''
        Get a full list, and output a .bat script to parallel download the last 'n' books.
        '''
        print("Getting Book List")
        booklist_resp = self.session.send(
            amazon_api.signed_request(
                "GET", 
                "https://todo-ta-g7g.amazon.com/FionaTodoListProxy/syncMetaData?&item_count=100&item_types=EBOK",
                state = self.auth_state,
            )
        )
        
        if booklist_resp.status_code != 200:
            print(booklist_resp.text)
            print("Could not acquire the booklist!")
            return False

        root = ET.fromstring(booklist_resp.text)
        asins = []
        print("Saving Book List...")
        f = open(os.path.join("download_new_book.bat"), "w")
        for meta in root.iter("meta_data"):
            asin = meta[0].text
            title = meta[1].text
            purchasedate = meta.find("purchase_date").text
            type = meta.find("origins")[0][0].text
            if not type == "Purchase":
                continue
            if n > 0:
                f.write(f'start "{asin}" cmd /K ""C:\Python311\python.exe" "%~dp0unkindle.py" --email {email} --keep_temp {asin}"\n')
                n = n - 1 
            asins.append((asin,purchasedate))    
        return asins


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="unkindle",
        description="Download and de-drm kindle books",
        epilog="https://github.com/athrowaway2021/comix",
        )
    
    parser.add_argument("-o", "--output", help="output folder")
    parser.add_argument("--downloadn", help="Save Severl Book")    
    parser.add_argument("--email", help="Login Email")        
    parser.add_argument("--loginonly", help="login only", action="store_true", default=False)
    parser.add_argument("--keep_temp", help="keep temp folder", action="store_true", default=True)
    parser.add_argument("--jpeg", help="output as jpeg", action="store_true", default=False)
    parser.add_argument("asin", help="asin of the book to download")
    args = parser.parse_args()

    # TODO: add subcommand to list available books for the account
    # can be done via signed_request(https://todo-ta-g7g.amazon.com/FionaTodoListProxy/syncMetaData?&item_count=1000&item_types=EBOK)
    
    # add by cunhan, for mutil-account.
    amazon_api.SCRIPT_PATH = os.path.join(amazon_api.SCRIPT_PATH, "state")
    Path(amazon_api.SCRIPT_PATH).mkdir(exist_ok=True)    
    amazon_api.STATE_PATH = os.path.join(amazon_api.SCRIPT_PATH, f"state[{args.email}].json")    
    print(amazon_api.STATE_PATH)
    
    kindle = Kindle(args.asin, hdv_png=not args.jpeg)
    if args.downloadn:
        kindle.get_book_list(int(args.downloadn), args.email)
    if not args.loginonly:
        if kindle.get_book():
            kindle.download(args.output, args.keep_temp)
