using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using AzReplicatorLibrary.TableEntities;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;


namespace AzReplicatorLibrary.DataLayer
{
    public class BlobStorage
    {
        CloudStorageAccount storageAccount;
        CloudBlobClient blobClient;
        CloudBlobContainer container;
        string storageAccountId;

        int retryCount = 0;

       
        public BlobStorage(CloudStorageAccount account, string containername)
        {
            storageAccount = account;
            storageAccountId = Common.MD5(storageAccount.BlobEndpoint.ToString());
            blobClient = storageAccount.CreateCloudBlobClient();
            container = blobClient.GetContainerReference(containername);
            container.CreateIfNotExistsAsync();
        }

        public async Task<CloudBlockBlob> UploadBlobAsync(byte[] data, string filename, bool compressed = true, bool overwrite = false, string contentType = "application/octet-stream")
        {
            return await UploadBlobAsync(retryCount, data, filename, compressed, overwrite, contentType);
        }

        private async Task<CloudBlockBlob> UploadBlobAsync(int retry, byte[] data, string filename, bool compressed = true, bool overwrite = false, string contentType = "application/octet-stream")
        {
            CloudBlockBlob blob = null;
            try
            {
                blob = container.GetBlockBlobReference(filename);

                if (!await blob.ExistsAsync() || overwrite)
                {
                    if (compressed)
                    {
                        using (MemoryStream comp = new MemoryStream())
                        {
                            using (GZipStream gzip = new GZipStream(comp, CompressionLevel.Optimal))
                            {
                                gzip.Write(data, 0, data.Length);
                                gzip.Close();
                            }

                            comp.Close();
                            data = comp.ToArray();
                        }
                    }

                    if (blob.Metadata.ContainsKey("compressed"))
                    {
                        blob.Metadata["compressed"] = compressed.ToString();
                    }
                    else
                    {
                        blob.Metadata.Add("compressed", compressed.ToString());
                    }

                    blob.Properties.ContentType = contentType;
                    await blob.UploadFromByteArrayAsync(data, 0, data.Length);
                }


            }
            catch (StorageException ex)
            {
                if (retry >= 1)
                {
                    retry--;
                    return await UploadBlobAsync(retry, data, filename, compressed, overwrite, contentType);
                }
                else
                {
                    Logger.TrackException(ex, 30, string.Format("Error uploading blob {0} to container {1}", filename, container.Name));
                }
            }
            return blob;
        }

        public async Task<byte[]> DownloadBlob(string filename)
        {
            try
            {
                CloudBlockBlob blob = container.GetBlockBlobReference(filename);
                byte[] data = null;
                await blob.DownloadToByteArrayAsync(data, 0);
                await blob.FetchAttributesAsync();

                if (data != null && Convert.ToBoolean(blob.Metadata["compressed"]))
                {
                    using (MemoryStream comp = new MemoryStream(data))
                    {
                        using (MemoryStream decomp = new MemoryStream())
                        {
                            using (GZipStream gzip = new GZipStream(comp, CompressionMode.Decompress))
                            {
                                gzip.CopyTo(decomp);
                                gzip.Close();
                            }
                            decomp.Close();
                            data = decomp.ToArray();
                        }
                        comp.Close();
                    }
                }

                return data;

            }
            catch (Exception ex)
            {
                Logger.TrackException(ex, 31, string.Format("Error downloading blob {0} to container {1}", filename, container.Name));
            }

            return null;
        }


    


        public async Task<List<KeyValuePair<string, Uri>>> ListBlobsUriWithSas(int readValidityInHours)
        {
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5);
            sasConstraints.SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(readValidityInHours);
            sasConstraints.Permissions = SharedAccessBlobPermissions.Read;

   
            BlobContinuationToken continuationToken = null;
            List<IListBlobItem> results = new List<IListBlobItem>();
            do
            {
                var response = await container.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results);
            }
            while (continuationToken != null);


            List<KeyValuePair<string, Uri>> blobs = new List<KeyValuePair<string, Uri>>();
            foreach (CloudBlockBlob blob in results)
            {
                string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);
                Uri sourceUri = new Uri(blob.Uri + sasBlobToken);
                blobs.Add(new KeyValuePair<string, Uri>(blob.Name, sourceUri));
            }

            return blobs;
        }

        public Uri GetReadBlobSasUri(string blobName, int readValidityInHours)
        {
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5);
            sasConstraints.SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(readValidityInHours);
            sasConstraints.Permissions = SharedAccessBlobPermissions.Read;

            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);
            Uri sourceUri = new Uri(blob.Uri + sasBlobToken);

            return sourceUri;

        }

    }
}
