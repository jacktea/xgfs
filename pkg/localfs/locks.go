package localfs

type lockRecord struct {
	owner     string
	exclusive bool
	ref       int
}
