package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	stationList = kingpin.Arg("list", "Station List").Required().String()
	dataDir     = kingpin.Arg("dir", "Data Directory").Required().String()
	rsltDir     = kingpin.Arg("result", "Result Directory").Required().String()
	fcstType    = kingpin.Arg("ftype", "Forecast Type [short, week]").Required().String()
)

type Station struct {
	Name      string
	Latitude  float32
	Longitude float32
	X         int
	Y         int
	Pos       int
}

type VarsInfo struct {
	Name string
	NL   int
}

type DataInfo struct {
	WesternLongitude float64
	EasternLogitude  float64
	SouthernLatitude float64
	NorthernLatitude float64
	NX               int
	NY               int
	NZ               int
	NT               int
	NVars            int
	Press            []float64
	Vars             []VarsInfo
	InitialTime      time.Time
}

func parseStationList(stationList string) map[string]*Station {
	station := map[string]*Station{}

	f, err := os.Open(stationList)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()

	sc := bufio.NewScanner(f)
	for i := 1; sc.Scan(); i++ {
		if err = sc.Err(); err != nil {
			log.Fatal(err)
		}
		w := strings.Split(sc.Text(), " ")
		stationID := w[0]
		lat, err := strconv.ParseFloat(w[1], 32)
		if err != nil {
			log.Fatal(err)
		}
		lon, err := strconv.ParseFloat(w[2], 32)
		if err != nil {
			log.Fatal(err)
		}
		name := w[3]
		station[stationID] = &Station{Name: name, Latitude: float32(lat), Longitude: float32(lon), X: 0, Y: 0}
	}

	return station
}

func calcStationPoint(station *map[string]*Station, di DataInfo) {
	if di.WesternLongitude < 0.0 {
		di.WesternLongitude = di.WesternLongitude + 360.0
	}
	if di.EasternLogitude < 0.0 {
		di.EasternLogitude = di.EasternLogitude + 360.0
	}
	if di.WesternLongitude > di.EasternLogitude {
		log.Fatal("Broken Control File")
	}

	di.SouthernLatitude = di.SouthernLatitude + 90.0
	di.NorthernLatitude = di.NorthernLatitude + 90.0
	if di.SouthernLatitude > di.NorthernLatitude {
		log.Fatal("Broken Control File")
	}

	for stationID, stationData := range *station {
		lat := stationData.Latitude + 90.0
		lon := stationData.Longitude
		if lon < 0 {
			lon = lon + 360.0
		}
		x := int((float64(lon) - di.WesternLongitude) / (di.EasternLogitude - di.WesternLongitude) * float64(di.NX))
		y := int((float64(lat) - di.SouthernLatitude) / (di.NorthernLatitude - di.SouthernLatitude) * float64(di.NY))

		if x < 0 || x > int(di.NX) {
			log.Println(stationID, "Out of range")
		}
		if y < 0 || y > int(di.NY) {
			log.Println(stationID, "Out of range")
		}
		stationData.X = x
		stationData.Y = y
		stationData.Pos = di.NX*y + x
	}
}

func getDataFiles(dir string, ft string) []string {
	var data []string

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), ft) && strings.HasSuffix(file.Name(), ".dat") {
			data = append(data, file.Name())
		}
	}
	return data
}

func pickup(dataDir string, dataFile string, stationList *map[string]*Station, di DataInfo, ft string) (map[string]map[string]map[float32][]float32, time.Time) {

	tmp := strings.TrimPrefix(dataFile, ft+"_")
	tmp = strings.TrimSuffix(tmp, ".dat")
	date := strings.Split(tmp, "_")[0]
	tm := strings.Split(tmp, "_")[1]
	year, err := strconv.Atoi(strings.Split(date, "-")[0])
	if err != nil {
		log.Fatal(err, date)
	}
	month, err := strconv.Atoi(strings.Split(date, "-")[1])
	if err != nil {
		log.Fatal(err, date)
	}
	day, err := strconv.Atoi(strings.Split(date, "-")[2])
	if err != nil {
		log.Fatal(err, date)
	}
	hour, err := strconv.Atoi(strings.Split(tm, ":")[0])
	if err != nil {
		log.Fatal(err, date)
	}
	minute, err := strconv.Atoi(strings.Split(tm, ":")[1])
	if err != nil {
		log.Fatal(err, date)
	}

	utc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}
	valid := time.Date(year, time.Month(month), day, hour, minute, 0, 0, utc)

	f, err := os.Open(dataDir + "/" + dataFile)
	if err != nil {
		log.Fatal(err)
	}

	extractVals := map[string]map[string]map[float32][]float32{}
	for _, variable := range di.Vars {
		name := variable.Name
		for i := 0; i < variable.NL; i++ {
			b := make([]byte, di.NX*di.NY*4)
			n, err := f.Read(b)
			if err != nil {
				log.Fatal(err)
			}
			if n != di.NX*di.NY*4 {
				log.Fatal("Less read")
			}
			v := make([]float32, di.NX*di.NY)
			if err = binary.Read(bytes.NewBuffer(b), binary.BigEndian, &v); err != nil {
				log.Fatal(err)
			}
			for id, station := range *stationList {
				p := station.Pos
				//log.Println(name, "(", di.Press[i], ")", id, ":", v[p])
				if _, ok := extractVals[id]; ok == false {
					extractVals[id] = make(map[string]map[float32][]float32)
				}
				if _, ok := extractVals[id][name]; ok == false {
					extractVals[id][name] = make(map[float32][]float32)
				}
				vals := []float32{v[p+di.NX-1], v[p+di.NX+0], v[p+di.NX+1], v[p+0-1], v[p+0+0], v[p+0+1], v[p-di.NX-1], v[p-di.NX+0], v[p-di.NX+1]}
				extractVals[id][name][float32(di.Press[i])] = vals
			}
		}
	}
	return extractVals, valid
}

func parseXDef(line string, sc *bufio.Scanner) (int, float64, float64) {
	tmp := strings.TrimPrefix(line, "xdef")
	tmp = strings.TrimSuffix(tmp, "levels")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}
	eLon := 0.0
	wLon := 0.0
	for i := 1; sc.Scan(); i++ {
		l := strings.TrimSpace(sc.Text())
		if i == 1 {
			wLon, err = strconv.ParseFloat(l, 32)
			if err != nil {
				log.Fatal(err)
			}
		}
		if i == num {
			eLon, err = strconv.ParseFloat(l, 32)
			if err != nil {
				log.Fatal(err)
			}
			break
		}
	}
	return num, wLon, eLon
}

func parseYDef(line string, sc *bufio.Scanner) (int, float64, float64) {
	tmp := strings.TrimPrefix(line, "ydef")
	tmp = strings.TrimSuffix(tmp, "levels")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}
	sLat := 0.0
	nLat := 0.0
	for i := 1; sc.Scan(); i++ {
		l := strings.TrimSpace(sc.Text())
		if i == 1 {
			sLat, err = strconv.ParseFloat(l, 32)
			if err != nil {
				log.Fatal(err)
			}
		}
		if i == num {
			nLat, err = strconv.ParseFloat(l, 32)
			if err != nil {
				log.Fatal(err)
			}
			break
		}
	}
	return num, sLat, nLat
}

func parseZDef(line string, sc *bufio.Scanner) (int, []float64) {
	var lev []float64
	tmp := strings.TrimPrefix(line, "zdef")
	tmp = strings.TrimSuffix(tmp, "levels")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; sc.Scan(); i++ {
		l := strings.TrimSpace(sc.Text())
		lv, err := strconv.ParseFloat(l, 32)
		if err != nil {
			log.Fatal(err)
		}
		lev = append(lev, lv)
		if i == num {
			break
		}
	}
	return num, lev
}

func parseVars(line string, sc *bufio.Scanner) (int, []VarsInfo) {
	var vars []VarsInfo
	tmp := strings.TrimPrefix(line, "VARS")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; sc.Scan(); i++ {
		l := sc.Text()
		if strings.HasPrefix(l, "ENDVARS") {
			break
		}

		nm := strings.Split(l, " ")[0]
		nu, err := strconv.Atoi(strings.Split(strings.TrimSpace(strings.Join(strings.Split(l, " ")[1:], " ")), " ")[0])
		if err != nil {
			log.Fatal(err)
		}
		vars = append(vars, VarsInfo{Name: nm, NL: nu})
	}
	return num, vars
}

func m3toMonth(m string) (time.Month, error) {
	switch m {
	case "JAN":
		return time.January, nil
	case "FEB":
		return time.February, nil
	case "MAR":
		return time.March, nil
	case "APR":
		return time.April, nil
	case "MAY":
		return time.May, nil
	case "JUN":
		return time.June, nil
	case "JUL":
		return time.July, nil
	case "AUG":
		return time.August, nil
	case "SEP":
		return time.September, nil
	case "OCT":
		return time.October, nil
	case "NOV":
		return time.November, nil
	case "DEC":
		return time.December, nil
	default:
		return -1, errors.New("Unknown Month")
	}
}

func parseTDef(line string, sc *bufio.Scanner) (int, time.Time) {
	tmp := strings.TrimPrefix(line, "tdef")
	num, err := strconv.Atoi(strings.Split(strings.TrimSpace(tmp), " ")[0])
	if err != nil {
		log.Fatal(err)
	}
	rest := strings.Split(strings.TrimSpace(strings.Join(strings.Split(strings.TrimSpace(tmp), " ")[1:], " ")), " ")[1]
	hour, err := strconv.Atoi(rest[0:2])
	if err != nil {
		log.Fatal(err)
	}
	day, err := strconv.Atoi(rest[3:5])
	if err != nil {
		log.Fatal(err)
	}
	month, err := m3toMonth(rest[5:8])
	if err != nil {
		log.Fatal(err)
	}

	year, err := strconv.Atoi(rest[8:12])
	if err != nil {
		log.Fatal(err)
	}

	utc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}

	return num, time.Date(year, month, day, hour, 0, 0, 0, utc)
}

func readControlFile(dir string, ft string) DataInfo {
	var di DataInfo
	fn := dir + "/" + ft + ".ctl"
	f, err := os.Open(fn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()

	sc := bufio.NewScanner(f)
	var nx, ny, nz, nt, nVars int
	var sLat, nLat, eLon, wLon float64
	var levs []float64
	var vars []VarsInfo
	var init time.Time

	for i := 1; sc.Scan(); i++ {
		if err = sc.Err(); err != nil {
			log.Fatal(err)
		}
		l := sc.Text()
		if strings.HasPrefix(l, "xdef") {
			nx, wLon, eLon = parseXDef(l, sc)
			continue
		}
		if strings.HasPrefix(l, "ydef") {
			ny, sLat, nLat = parseYDef(l, sc)
			continue
		}
		if strings.HasPrefix(l, "zdef") {
			nz, levs = parseZDef(l, sc)
			continue
		}
		if strings.HasPrefix(l, "tdef") {
			nt, init = parseTDef(l, sc)
			continue
		}
		if strings.HasPrefix(l, "VARS") {
			nVars, vars = parseVars(l, sc)
			continue
		}
	}
	di.EasternLogitude = eLon
	di.WesternLongitude = wLon
	di.NorthernLatitude = nLat
	di.SouthernLatitude = sLat
	di.NX = nx
	di.NY = ny
	di.NZ = nz
	di.NT = nt
	di.NVars = nVars
	di.Press = levs
	di.Vars = vars
	di.InitialTime = init
	return di
}

func main() {
	kingpin.Parse()

	stationList := parseStationList(*stationList)
	di := readControlFile(*dataDir, *fcstType)

	calcStationPoint(&stationList, di)
	dataFiles := getDataFiles(*dataDir, *fcstType)
	for _, dataFile := range dataFiles {
		val, tm := pickup(*dataDir, dataFile, &stationList, di, *fcstType)
		msg, err := msgpack.Marshal(val)
		if err != nil {
			log.Fatal(err)
		}

		d := *rsltDir + "/" + di.InitialTime.Format("20060102_150405")

		if err = os.MkdirAll(d, 0777); err != nil {
			log.Fatal(err)
		}

		f, err := os.Create(d + "/" + tm.Format("20060102_150405") + ".msg")
		if err != nil {
			log.Fatal(err)
		}
		if _, err := f.Write(msg); err != nil {
			log.Fatal(err)
		}

		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}
}
