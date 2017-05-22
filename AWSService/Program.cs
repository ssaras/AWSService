using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;

using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.SimpleDB;
using Amazon.SimpleDB.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System.Diagnostics;

namespace AWSService
{
    class Program
    {
        // I have a bucket called 'shawnlocal' on aws
        static string existingBucketName = "shawnlocal";
        static string keyName = "TestKeyName";
        static string filePath = string.Empty;

        public static void Main(string[] args)
        {            
            Console.WriteLine("===========================================");
            Console.WriteLine("      Welcome to the AWS .NET SDK!         ");
            Console.WriteLine("===========================================");
            Console.WriteLine("");
            Console.WriteLine("Please provide a file path:");

            /*
             * The location of the file to be uploaded 
             */
            filePath = Console.ReadLine();

            /* 
             * Tests file upload by breaking it into 0 chunks  
             */
            Console.WriteLine("");
            UploadWithoutMultipart(filePath);

            /* 
             * Tests file upload by breaking it into 5 chunks  
             */
            //Console.WriteLine("");
            //UploadWithMultipart(filePath, 5);

            /* 
             * Tests file upload by breaking it into 10 chunks  
             */
            //Console.WriteLine("");
            //UploadWithMultipart(filePath, 10);

            Console.Read();
        }

        /// <summary>
        /// Uploads the file without breaking it into chunks
        /// </summary>
        /// <param name="filePath">The file location on the user's machine</param>
        public static void UploadWithoutMultipart (string filePath)
        {
            TransferUtility utility = new TransferUtility(new AmazonS3Client(Amazon.RegionEndpoint.USEast1));
            Stopwatch stopWatch = new Stopwatch();

            Console.WriteLine("START NO MULTIPART: " + DateTime.Now);

            stopWatch.Start();
            utility.Upload(filePath, existingBucketName);
            stopWatch.Stop();

            Console.WriteLine("FINISH NO MULTIPART: " + DateTime.Now);
            Console.WriteLine("TOTAL NO MULTIPART: " + stopWatch.Elapsed);
        }

        /// <summary>
        /// uploads a file by breaking it into chunks
        /// </summary>
        /// <param name="filePath">The file location on the user's machine</param>
        /// <param name="partLength">The size of chunks (in MB) the file should be broken up into</param>
        public static void UploadWithMultipart(string filePath, int partLength)
        {
            Stopwatch stopWatch = new Stopwatch();
            IAmazonS3 s3Client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);
            List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();

            Console.WriteLine("START WITH MULTIPART: " + DateTime.Now);
            stopWatch.Start();            

            InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = existingBucketName,
                Key = keyName
            };

            InitiateMultipartUploadResponse initResponse = s3Client.InitiateMultipartUpload(initiateRequest);

            long contentLength = new FileInfo(filePath).Length;
            long partSize = partLength * (long)Math.Pow(2, 20); // 5 MB

            Console.WriteLine("File Size: " + contentLength);
            Console.WriteLine("Part Size: " + partSize);

            try
            {
                long filePosition = 0;
                for (int i = 1; filePosition < contentLength; i++)
                {
                    Console.WriteLine("Starting Part: : " + i);
                    UploadPartRequest uploadRequest = new UploadPartRequest
                    {
                        BucketName = existingBucketName,
                        Key = keyName,
                        UploadId = initResponse.UploadId,
                        PartNumber = i,
                        PartSize = partSize,
                        FilePosition = filePosition,
                        FilePath = filePath
                    };

                    // Upload part and add response to our list.
                    uploadResponses.Add(s3Client.UploadPart(uploadRequest));

                    filePosition += partSize;
                }

                // Step 3: complete.
                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest
                {
                    BucketName = existingBucketName,
                    Key = keyName,
                    UploadId = initResponse.UploadId,
                    //PartETags = new List<PartETag>(uploadResponses)

                };
                completeRequest.AddPartETags(uploadResponses);

                CompleteMultipartUploadResponse completeUploadResponse = s3Client.CompleteMultipartUpload(completeRequest);

            }
            catch (Exception exception)
            {
                Console.WriteLine("Exception occurred: {0}", exception.Message);
                AbortMultipartUploadRequest abortMPURequest = new AbortMultipartUploadRequest
                {
                    BucketName = existingBucketName,
                    Key = keyName,
                    UploadId = initResponse.UploadId
                };
                s3Client.AbortMultipartUpload(abortMPURequest);
            }
                                    
            stopWatch.Stop();
            Console.WriteLine("END WITH MULTIPART: " + DateTime.Now);
            Console.WriteLine("TOTAL WITH MULTIPART: " + stopWatch.Elapsed);            
        }        
        
    }
}