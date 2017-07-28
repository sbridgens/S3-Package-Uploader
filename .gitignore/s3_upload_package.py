#!/usr/bin/python
import os
import sys
import argparse
import boto
from boto.s3.key import Key
from os.path import basename
from boto.s3.connection import S3Connection
from multiprocessing.pool import ThreadPool

#Test Items
#python test.py --materialID "SOMEFILENAME" --s3KeyID "..."  --s3Secret "...." --s3BaseKey "somebasekey" --s3Bucket "somebuckey" --s3_sub_key ""
#
#
TEMP_DIR_PATH = "/mnt/output_package/"
TOTAL_PROCESSES = 20

class UploadS3Package(object):
    def __init__(self):
        self._parse_options()
        self._check_package_exists()
        self._enumerate_package()
        self._MT_Process()
        self._exit_code=0
        self._uploadFilesArray

    def _parse_options(self):
        parser = argparse.ArgumentParser(description="S3 Package Upload Script.")
        parser.add_argument('--materialID', type=str, required=True)
        parser.add_argument('--s3KeyID', type=str, required=True)
        parser.add_argument('--s3Secret', type=str, required=True)
        parser.add_argument('--s3BaseKey', type=str, required=True)
        parser.add_argument('--s3Bucket', type=str, required=True)
        parser.add_argument('--s3_sub_key', type=str, required=True)
        self.args = parser.parse_args()


    def _check_package_exists(self):
        tmpDir = ''.join(TEMP_DIR_PATH + self.args.materialID)
        print tmpDir
        if not os.path.exists(tmpDir):
            return False
        else:
            return True


    def _enumerate_package(self):
        self._uploadFilesArray = []
        for root, dirs, files in os.walk(self.args.materialID, topdown=False):
            for name in files:
                if self.args.s3_sub_key == "0":
                    self._uploadFilesArray.append(name)
                else:
                    fname=os.path.join(root, name)
                    self._uploadFilesArray.append(fname)


       
    def _upload_to_S3(self, upload_file):
        print "Uploading Files In Progress...."
        sys.stdout.flush()

        try:
            upconn = S3Connection(self.args.s3KeyID, self.args.s3Secret)
            upBucket = upconn.get_bucket(self.args.s3Bucket)
            upKey = Key(upBucket)
            upKey.key = ''.join(self.args.s3BaseKey + "/" + upload_file)
            upKey.set_contents_from_filename(upload_file)
            #print upKey
            return upload_file
        except Exception as uex:
            print uex
            self._exit_code = 1


    def _MT_Process(self):
        pool = ThreadPool(processes=TOTAL_PROCESSES)
        pool.map(self._upload_to_S3, self._uploadFilesArray)
        pool.close()
        pool.join()


    def _exit_program(self, msg):
        print msg
        sys.stdout.flush()
        exit(self._exit_code)
        
    
    def main(self):
        #Check the package exists on disk, else fail
        if self._check_package_exists() == False:
            self._exit_code = 1
            self._exit_program("ERROR: Package {0} Does not Exist!".format(self.args.materialID))
        else:
            #change to package dir as working path
            os.chdir(TEMP_DIR_PATH)
            #list all files/directories in the package
            self._enumerate_package()
            #check the file_array is populated
            if not self._uploadFilesArray:
                #fail the job
                self._exit_code = 2
                self._exit_program("ERROR: Source Media Files Not Found!")
            else:
                print "Starting Upload of Package: %s" % self.args.materialID
                sys.stdout.flush()
                if self.args.s3_sub_key == "0":
                    tmpDir = os.path.join(TEMP_DIR_PATH, self.args.materialID)
                    os.chdir(tmpDir)
                #start the upload process
                upload_start = self._MT_Process()

                if self._exit_code == 0:
                    self._exit_program("SUCCESS: Package {0} Successfully Uploaded To S3".format(self.args.materialID))
                else:
                    self._exit_code = 3
                    self._exit_program("ERROR: Upload of Package: {0} to S3 FAILED!".format(self.args.materialID))

                    

def main():
    upload_package = UploadS3Package()
    upload_package.main()

if __name__ == "__main__":
    main()


