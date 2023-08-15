// Send @RSYNCD x.x\n
// Send modname\n
// Send arugment with mod list\0	filter list write(0)    \n
// handshake
// batch seed
// Recv file list
//

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"rsync-os/rsync"
	"rsync-os/storage"
	"strings"
	"time"
)

func ClientS3(src string, dest string) {
	addr, module, pth, err := rsync.SplitURI(src)

	if err != nil {
		log.Println("Invaild URI")
		return
	}

	log.Println(module, pth)

	ppath := rsync.TrimPrepath(pth)

	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	keyaccess := os.Getenv("AWS_ACCESS_KEY_ID")
	keysecret := os.Getenv("AWS_SECRET_ACCESS_KEY")

	dest = path.Clean(dest)
	bucketAndPrefix := strings.SplitN(dest, "/", 2)

	stor, _ := storage.NewMinio(bucketAndPrefix[0], bucketAndPrefix[1]+"/"+ppath, endpoint, keyaccess, keysecret)
	defer stor.Close()

	client, err := rsync.SocketClient(stor, addr, module, ppath, nil)
	if err != nil {
		panic("rsync client fails to initialize")
	}
	if err := client.Sync(); err != nil {
		panic(err)
	}

}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("Usage: rsync-os [OPTION]... rsync://[USER@]HOST[:PORT]/SRC bucket/prefix/path")
		return
	}
	startTime := time.Now()
	ClientS3(args[0], args[1])
	log.Println("Duration:", time.Since(startTime))
}
