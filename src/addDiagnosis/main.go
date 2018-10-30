package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	ctrlFile = kingpin.Arg("file", "GrADS Control File").Required().String()
	dataDir  = kingpin.Arg("dir", "Data Directory").Required().String()
)

type VarsInfo struct {
	Name string
	NL   int
}

func parseXDef(line string) int {
	tmp := strings.TrimPrefix(strings.TrimSpace(line), "xdef")
	tmp = strings.TrimSuffix(strings.TrimSpace(tmp), "levels")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}
	return num
}

func parseYDef(line string) int {
	tmp := strings.TrimPrefix(strings.TrimSpace(line), "ydef")
	tmp = strings.TrimSuffix(strings.TrimSpace(tmp), "levels")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}
	return num
}

func parseVars(line string, sc *bufio.Scanner, fw *os.File) (int, []VarsInfo) {
	var vars []VarsInfo
	tmp := strings.TrimPrefix(strings.TrimSpace(line), "VARS")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}

	fw.WriteString(fmt.Sprintf("VARS %d\n", num+3))

	for i := 1; sc.Scan(); i++ {
		l := sc.Text()
		if strings.HasPrefix(l, "ENDVARS") {
			fw.WriteString("SSI      1 0 SSI\n")
			fw.WriteString("SSI8570  1 0 SSI 850-700\n")
			fw.WriteString("SSI9285  1 0 SSI 925-850\n")
			fw.WriteString("ENDVARS\n")
			break
		}
		fw.WriteString(l + "\n")
		nm := strings.Split(l, " ")[0]
		nu, err := strconv.Atoi(strings.Split(strings.TrimSpace(strings.Join(strings.Split(l, " ")[1:], " ")), " ")[0])
		if err != nil {
			log.Fatal(err)
		}
		vars = append(vars, VarsInfo{Name: nm, NL: nu})
	}
	return num, vars
}

func parseZDef(line string, sc *bufio.Scanner, fw *os.File) (int, []float64) {
	var lev []float64
	tmp := strings.TrimPrefix(strings.TrimSpace(line), "zdef")
	tmp = strings.TrimSuffix(strings.TrimSpace(tmp), "levels")
	num, err := strconv.Atoi(strings.TrimSpace(tmp))
	if err != nil {
		log.Fatal(err)
	}

	fw.WriteString(fmt.Sprintf("zdef %d levels\n", num))

	for i := 1; sc.Scan(); i++ {
		l := strings.TrimSpace(sc.Text())
		fw.WriteString(l + "\n")
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

func parseCtlFile(file string) (int, int, int, int, []float64, []VarsInfo) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	fw, err := os.Create(file + ".tmp")
	if err != nil {
		log.Fatal(err)
	}

	sc := bufio.NewScanner(f)

	var nx, ny, nz, nVar int
	var vars []VarsInfo
	var levels []float64

	for sc.Scan() {
		if err = sc.Err(); err != nil {
			log.Fatal(err)
		}

		l := sc.Text()
		if strings.HasPrefix(l, "xdef") {
			nx = parseXDef(l)
		}
		if strings.HasPrefix(l, "ydef") {
			ny = parseYDef(l)
		}
		if strings.HasPrefix(l, "zdef") {
			nz, levels = parseZDef(l, sc, fw)
			continue
		}
		if strings.HasPrefix(l, "VARS") {
			nVar, vars = parseVars(l, sc, fw)
			continue
		}
		if _, err := fw.WriteString(l + "\n"); err != nil {
			log.Fatal(err)
		}
	}

	fw.Close()
	f.Close()

	if err := os.Remove(file); err != nil {
		log.Fatal(err)
	}
	if err := os.Rename(file+".tmp", file); err != nil {
		log.Fatal(err)
	}

	return nx, ny, nz, nVar, levels, vars
}

func calc_ept(p, t, td float32) float32 {
	t = t + 273.15
	td = td + 273.15

	term1 := float64(1.0 / (td - 56.0))
	term2 := math.Log(float64(t/td)) / 800.0
	Tlcl := 1.0/(term1+term2) + 56.0

	e := 6.112 * math.Exp(float64((17.67*(td-273.15))/(td-29.65)))
	//es := 6.112 * math.Exp(float64((17.67*(t-273.15))/(t-29.65)))
	x := 0.622 * (e / (float64(p) - e))

	return float32(float64(t) * math.Pow(1000.0/(float64(p)-e), 0.2854) * math.Pow(float64(t)/Tlcl, 0.28*x) * math.Exp((3036.0/Tlcl-1.78)*x*(1.0+0.448*x)))
}

func calc_ssi(lowT, lowTd, upT []float32, lowP, upP float32, nx, ny int) []float32 {
	ssi := make([]float32, nx*ny)
	for x := 0; x < nx; x++ {
		for y := 0; y < ny; y++ {
			lowEPT := calc_ept(lowP, lowT[x+nx*y], lowTd[x+nx*y])
			t1 := float32(-80.0)
			t2 := float32(80.0)
			loop := 0

			for true {
				ept1 := calc_ept(upP, t1, t1)
				er1 := ept1 - lowEPT

				if math.IsNaN(float64(ept1)) {
					t2 = ept1
					break
				}
				if loop == 10000 {
					t2 = float32(math.Pow10(32))
					break
				}
				if math.Abs(float64(er1)) < 0.001 {
					break
				}

				ept2 := calc_ept(upP, t2, t2)
				er2 := ept2 - lowEPT
				if (er1 * er2) < 0.0 {
					t2 = t2 - (t2-t1)/2.0
				} else {
					t3 := t1
					t1 = t2
					t2 = t2 + (t2-t3)/2.0
				}

				loop = loop + 1
			}

			if math.IsNaN(float64(t2)) {
				ssi[x+nx*y] = float32(math.Pow10(32))
			} else {
				ssi[x+nx*y] = upT[x+nx*y] - t2
			}
		}
	}
	return ssi
}

func calc_and_write_SSI(fn string, nx, ny int, n500, n700, n850, n925 int, tC, tdC int) {
	file, err := os.OpenFile(fn, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()

	buf := make([]byte, nx*ny*4)
	t500 := make([]float32, nx*ny)
	t700 := make([]float32, nx*ny)
	t850 := make([]float32, nx*ny)
	t925 := make([]float32, nx*ny)
	td500 := make([]float32, nx*ny)
	td700 := make([]float32, nx*ny)
	td850 := make([]float32, nx*ny)
	td925 := make([]float32, nx*ny)

	tBase := tC * (nx * ny)
	if _, err := file.ReadAt(buf, int64((tBase+n500*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &t500); err != nil {
		log.Fatal(err)
	}

	if _, err := file.ReadAt(buf, int64((tBase+n700*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &t700); err != nil {
		log.Fatal(err)
	}

	if _, err := file.ReadAt(buf, int64((tBase+n850*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &t850); err != nil {
		log.Fatal(err)
	}

	if _, err := file.ReadAt(buf, int64((tBase+n925*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &t925); err != nil {
		log.Fatal(err)
	}

	tdBase := tdC * (nx * ny)
	if _, err := file.ReadAt(buf, int64((tdBase+n500*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &td500); err != nil {
		log.Fatal(err)
	}
	if _, err := file.ReadAt(buf, int64((tdBase+n700*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &td700); err != nil {
		log.Fatal(err)
	}
	if _, err := file.ReadAt(buf, int64((tdBase+n850*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &td850); err != nil {
		log.Fatal(err)
	}
	if _, err := file.ReadAt(buf, int64((tdBase+n925*(nx*ny))*4)); err != nil {
		log.Fatal(err)
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &td925); err != nil {
		log.Fatal(err)
	}

	ssi85 := calc_ssi(t850, td850, t500, 850.0, 500.0, nx, ny)
	ssi87 := calc_ssi(t850, td850, t700, 850.0, 700.0, nx, ny)
	ssi98 := calc_ssi(t925, td925, t850, 925.0, 850.0, nx, ny)

	if err := binary.Write(bytes.NewBuffer(buf), binary.BigEndian, ssi85); err != nil {
		log.Fatal(err)
	}
	if _, err := file.Write(buf); err != nil {
		log.Fatal(err)
	}
	if err := binary.Write(bytes.NewBuffer(buf), binary.BigEndian, ssi87); err != nil {
		log.Fatal(err)
	}
	if _, err := file.Write(buf); err != nil {
		log.Fatal(err)
	}
	if err := binary.Write(bytes.NewBuffer(buf), binary.BigEndian, ssi98); err != nil {
		log.Fatal(err)
	}
	if _, err := file.Write(buf); err != nil {
		log.Fatal(err)
	}

}

func main() {
	kingpin.Parse()

	nx, ny, _, _, levels, vars := parseCtlFile(*ctrlFile)

	var n850, n700, n925, n500 int
	for n, v := range levels {
		if v == 500.0 {
			n500 = n
		} else if v == 700.0 {
			n700 = n
		} else if v == 850.0 {
			n850 = n
		} else if v == 925.0 {
			n925 = n
		}
	}

	var tC, tdC int
	c := 0
	for _, v := range vars {
		if v.Name == "tc" {
			tC = c
		} else if v.Name == "td" {
			tdC = c
		}
		c = c + v.NL
	}

	files, err := ioutil.ReadDir(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".dat") {
			go calc_and_write_SSI(*dataDir+"/"+file.Name(), nx, ny, n500, n700, n850, n925, tC, tdC)
		}
	}

}
