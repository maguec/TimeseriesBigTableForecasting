package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"math/rand"

	"cloud.google.com/go/bigtable"
	"github.com/alexflint/go-arg"
	"go.uber.org/ratelimit"
)

type Data struct {
	Timestamp int
	Ip        string
}

var ctx = context.Background()

var args struct {
	Project  string `help:"GCP Project to use" default:"" arg:"--project, -p, env:BT_PROJECT"`
	Instance string `help:"BT Instance to use" default:"timeseries" arg:"--instance, -i, env:BT_INSTANCE"`
	Database string `help:"BT Database to use" default:"metrics" arg:"--database, -d, env:BT_DATABSE"`
	Table    string `help:"BT Table to use" default:"stats" arg:"--table, -t, env:BT_TABLE"`
	RPS      int    `help:"Number of updates per second " default:"1000" arg:"--rps, -r, env:BT_RPS"`
	Records  int    `help:"Toal Number of records to write" default:"10000" arg:"--records, -w, env:BT_RECORDS"`
	Threads  int    `help:"Number of threads to concurrent write" default:"30" arg:"--threads, -z, env:BT_THREADS"`
	Verbose  bool   `help:"Show verbose output" default:"false" arg:"--verbose, -v, env:BT_VERBOSE"`
	Cidr     string `help:"CIDR to use" default:"10.0.0.0/8" arg:"--cidr, -c, env:BT_CIDR"`
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}

func cidrIPs(cidr string) ([]string, error) {
	ipv4Addr, ipv4Net, err := net.ParseCIDR(cidr)
	if err != nil {
		return []string{}, err
	}

	var ips []string
	for ip := ipv4Addr.Mask(ipv4Net.Mask); ipv4Net.Contains(ip); inc(ip) {
		ips = append(ips, ip.String())
	}
	usableIps := ips[1 : len(ips)-1]
	return usableIps, nil
}

func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func createTable(project, instance, table, columnFamily string, debug bool) error {
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		return fmt.Errorf("Admin Client(%s:%s): %v", project, instance, err)
	}

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return fmt.Errorf("List Tables(%s): %v", instance, err)
	}

	if !sliceContains(tables, table) {
		if debug {
			log.Printf("Creating table %s", table)
		}
		if err := adminClient.CreateTable(ctx, table); err != nil {
			return fmt.Errorf("CreateTable(%s): %v", table, err)
		}
		if debug {
			log.Printf("Creating column family %s", columnFamily)
		}
		if err := adminClient.CreateColumnFamily(ctx, table, columnFamily); err != nil {
			return fmt.Errorf("CreateColumnFamily(%s): %v", columnFamily, err)
		}
		maxAge := time.Hour * 24
		policy := bigtable.MaxAgePolicy(maxAge)
		if err := adminClient.SetGCPolicy(ctx, table, columnFamily, policy); err != nil {
			return fmt.Errorf("SetGCPolicy(%s): %v", policy, err)
		}
		if debug {
			log.Printf("GCP Policy set on %s", table)
		}
	}

	return nil
}

func writeWorker(
	id int,
	jobs <-chan Data,
	results chan<- time.Duration,
	rl ratelimit.Limiter,
	verbose bool,
	project, instance, table, family string) {

	if verbose {
		log.Printf("Starting write worker: %d\n", id)
	}
	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		log.Fatalf("bigtable.NewAdminClient: %v", err)
	}
	tbl := client.Open(table)

	for j := range jobs {
		rl.Take()
		startTime := time.Now()
		mut := bigtable.NewReadModifyWrite()
		mut.Increment("stats", "ips", 1)

		if _, err := tbl.ApplyReadModifyWrite(ctx, j.Ip, mut); err != nil {
			log.Fatalf("Job: %v Apply: %v Family: %v", j, err, family)
		}

		if verbose {
			log.Printf("Job: %v", j)
		}

		results <- time.Since(startTime)
	}
	client.Close()
}

func main() {
	arg.MustParse(&args)
	err := createTable(
		args.Project,
		args.Instance,
		args.Database,
		args.Table,
		args.Verbose,
	)

	rl := ratelimit.New(args.RPS)

	if args.Project == "" {
		log.Fatal("Please specificy project, instance, database and table")
	}

	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}

	jobs := make(chan Data, args.Records)
	res := make(chan time.Duration, args.Records)

	ips, err := cidrIPs(args.Cidr)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < args.Records; i++ {
		ip := ips[rand.Intn(len(ips))]
		if err != nil {
			log.Fatal(err)
		}
		jobs <- Data{
			Timestamp: int(time.Now().Unix()),
			Ip:        ip,
		}
	}

	if args.Verbose {
		log.Printf("Writing of %d records started", args.Records)
	}

	for w := 1; w <= args.Threads; w++ {
		go writeWorker(
			w, jobs, res, rl,
			args.Verbose, args.Project,
			args.Instance, args.Database, args.Table)
	}

	for a := 0; a < args.Records; a++ {
		<-res
	}

	if args.Verbose {
		log.Printf("Writing of %d records complete", args.Records)
	}

}
