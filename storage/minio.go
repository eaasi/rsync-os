package storage

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"rsync-os/rsync"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v6"
)

/*
rsync-os will add addition information for each file that was uploaded to minio
rsync-os stores the information of a folder in the metadata of an empty file called "..."
rsync-os also uses a strange file to represent a soft link
*/

// S3 with cache
type Minio struct {
	client     *minio.Client
	bucketName string
	prefix     string
}

func NewMinio(bucket string, prefix string, endpoint string, accessKeyID string, secretAccessKey string, secure bool) (*Minio, error) {
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, false)
	if os.Getenv("debug") != "" {
		minioClient.TraceOn(nil)
	}
	if err != nil {
		panic("Failed to init a minio client")
	}
	// Create a bucket for the module
	err = minioClient.MakeBucket(bucket, "us-east-1")
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(bucket)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucket)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s (depending on the S3 provider, the bucket also may already have existed)\n", bucket)
	}

	return &Minio{
		client:     minioClient,
		bucketName: bucket,
		prefix:     prefix,
	}, nil
}

// object can be a regualar file, folder or symlink
func (m *Minio) Put(fileName string, content io.Reader, fileSize int64, metadata rsync.FileMetadata) (written int64, err error) {
	data := make(map[string]string)
	data["original-last-modified"] = time.Unix(int64(metadata.Mtime), 0).UTC().Format(http.TimeFormat)
	data["original-file-mode"] = fmt.Sprintf("%#o", metadata.Mode)
	for k, v := range metadata.User {
		if v != "" {
			data[k] = v
		}
	}

	fpath := filepath.Join(m.prefix, fileName)
	fsize := fileSize
	fname := fpath
	if metadata.Mode.IsDIR() {
		fsize = 0
		return
	}

	if metadata.Mode.IsLNK() {
		// Additional data of symbol link
	}

	written, err = m.client.PutObject(m.bucketName, fname, content, fsize, minio.PutObjectOptions{UserMetadata: data})

	return
}

func (m *Minio) Delete(fileName string, mode rsync.FileMode) (err error) {
	fpath := filepath.Join(m.prefix, fileName)
	// TODO: How to delete a folder
	if mode.IsDIR() {
		return
	}
	if err = m.client.RemoveObject(m.bucketName, fpath); err != nil {
		return
	}
	log.Println(fileName)
	return nil
}

func (m *Minio) List() (rsync.FileList, error) {
	filelist := make(rsync.FileList, 0, 1<<16)

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// FIXME: objectPrefix, recursive
	objectCh := m.client.ListObjectsV2(m.bucketName, m.prefix, true, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Println(object.Err)
			return filelist, object.Err
		}

		// FIXME: Handle folder
		objectName := object.Key[len(m.prefix):]
		if strings.Compare(path.Base(objectName), "...") == 0 {
			objectName = path.Dir(objectName)
		}

		// Have to rely on Last-Modified date as most S3 providers do not provide user metadata in ListObjectsV2
		mtime := int(object.LastModified.Unix())
		mode := 0

		filelist = append(filelist, rsync.FileInfo{
			Path:  []byte(objectName),
			Size:  object.Size,
			Mtime: int32(mtime),
			Mode:  rsync.FileMode(mode),
		})
	}

	sort.Sort(filelist)

	return filelist, nil
}

func (m *Minio) Close() error {
	return nil
}
