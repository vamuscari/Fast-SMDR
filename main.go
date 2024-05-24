package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

func main() {
	var port int = 514
	var err error
	var filter net.IP
	var dbconn string

	slog.SetDefault(logger)

	// Input Args
	for i, v := range os.Args {
		switch v {
		// Port
		case "-p":
			port, err = strconv.Atoi(os.Args[(i + 1)])
			if err != nil {
				log.Fatalln(err)
			}
			// Filter address
		case "-f":
			filter = net.ParseIP(os.Args[(i + 1)])
			// Database string
		case "-d":
			dbconn = os.Args[(i + 1)]
		}
	}

	ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalln(err)
		return
	}

	slog.Info(fmt.Sprintf("Listening on %s\n", ln.Addr().String()))

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("Failed to Accept Incoming", slog.Any("err", err))
			continue
		}

		if checkConnection(conn, filter) {
			go handleConnection(conn, dbconn)
		} else {
			conn.Close()
		}

	}
}

// CheckConnection prevents any unwanted incomming logs from an ip or port
func checkConnection(conn net.Conn, filter net.IP) bool {
	if filter == nil {
		return true
	}

	if strings.Contains(conn.RemoteAddr().String(), filter.String()) {
		return true
	}
	slog.Warn(fmt.Sprintf("Blocked Connection From: %s\n", conn.RemoteAddr().String()))
	return false
}

func handleConnection(conn net.Conn, dbconn string) {
	defer conn.Close()

	buf := make([]byte, 2096)
	_, err := conn.Read(buf)
	if err != nil {
		slog.Error("Failed to handle connection",
			slog.String("function", "conn.Read"),
			slog.Any("err", err))
		return
	}

	smdr, err := parseBuffer(buf)
	if err != nil {
		slog.Error("Failed to handle connection",
			slog.String("function", "parseBuffer"),
			slog.Any("err", err))
	}

	db := pgInit(dbconn)
	pgInsertSMDR(db, smdr)

}

type SMDR_Packet struct {
	CallStart                   sql.NullTime    `db:"CallStart"`
	ConnectedTime               pgtype.Interval `db:"ConnectedTime"`
	RingTime                    sql.NullInt64   `db:"RingTime"`
	Caller                      sql.NullString  `db:"Caller"`
	CallDirection               sql.NullString  `db:"CallDirection"`
	CalledNumber                sql.NullString  `db:"CalledNumber"`
	DialedNumber                sql.NullString  `db:"DialedNumber"`
	Account                     sql.NullString  `db:"Account"`
	IsInternal                  sql.NullBool    `db:"IsInternal"`
	CallId                      sql.NullInt64   `db:"CallId"`
	Continuation                sql.NullBool    `db:"Continuation"`
	Party1Device                sql.NullString  `db:"Party1Device"`
	Party1Name                  sql.NullString  `db:"Party1Name"`
	Party2Device                sql.NullString  `db:"Party2Device"`
	Party2Name                  sql.NullString  `db:"Party2Name"`
	ExternalTargeterId          sql.NullString  `db:"ExternalTargeterId"`
	HoldTime                    sql.NullInt64   `db:"HoldTime"`
	ParkTime                    sql.NullInt64   `db:"ParkTime"`
	AuthValid                   sql.NullString  `db:"AuthValid"`
	AuthCode                    sql.NullString  `db:"AuthCode"`
	UserCharged                 sql.NullString  `db:"UserCharged"`
	CallCharge                  sql.NullString  `db:"CallCharge"`
	Currency                    sql.NullString  `db:"Currency"`
	AmountatLastUserChange      sql.NullString  `db:"AmountatLastUserChange"`
	CallUnits                   sql.NullString  `db:"CallUnits"`
	UnitsatLastUserChange       sql.NullString  `db:"UnitsatLastUserChange"`
	CostperUnit                 sql.NullString  `db:"CostperUnit"`
	MarkUp                      sql.NullString  `db:"MarkUp"`
	ExternalTargetingCause      sql.NullString  `db:"ExternalTargetingCause"`
	ExternalTargetedNumber      sql.NullString  `db:"ExternalTargetedNumber"`
	CallingPartyServerIpAddress sql.NullString  `db:"CallingPartyServerIpAddress"`
	UniqueCallIDForTheCallerExt sql.NullString  `db:"UniqueCallIDForTheCallerExt"`
	CalledPartyServerIP         sql.NullString  `db:"CalledPartyServerIP"`
	UniqueCallIDforCalledExt    sql.NullString  `db:"UniqueCallIDforCalledExt"`
	SMDRRecordTime              sql.NullTime    `db:"SMDRRecordTime"`
	CallerConsentDirective      sql.NullString  `db:"CallerConsentDirective"`
	CallingNumberVerification   sql.NullString  `db:"CallingNumberVerification"`
	Undefined                   sql.NullString  `db:"Undefined"`
}

func parseBuffer(buf []byte) (SMDR_Packet, error) {

	var smdr SMDR_Packet

	//  0 \r\n\u0000\u0000
	str := string(buf)
	newline := strings.Index(str, "\n")
	if newline > 0 {
		str = str[:newline]
	}
	arr := strings.Split(str, ",")
	if len(arr) != 38 {
		for i, v := range arr {
			fmt.Printf("%d: %s\n", (i + 1), v)
		}
		return smdr, errors.New(fmt.Sprintf("parseBuffer: %d columns received instead of 38", len(arr)))
	}

	smdr = SMDR_Packet{
		CallStart:                   validateDateTime(arr[0]),
		ConnectedTime:               validateInterval(arr[1]),
		RingTime:                    validateInt64(arr[2]),
		Caller:                      validateString(arr[3]),
		CallDirection:               validateString(arr[4]),
		CalledNumber:                validateString(arr[5]),
		DialedNumber:                validateString(arr[6]),
		Account:                     validateString(arr[7]),
		IsInternal:                  validateBool(arr[8]),
		CallId:                      validateInt64(arr[9]),
		Continuation:                validateBool(arr[10]),
		Party1Device:                validateString(arr[11]),
		Party1Name:                  validateString(arr[12]),
		Party2Device:                validateString(arr[13]),
		Party2Name:                  validateString(arr[14]),
		ExternalTargeterId:          validateString(arr[15]),
		HoldTime:                    validateInt64(arr[16]),
		ParkTime:                    validateInt64(arr[17]),
		AuthValid:                   validateString(arr[18]),
		AuthCode:                    validateString(arr[19]),
		UserCharged:                 validateString(arr[20]),
		CallCharge:                  validateString(arr[21]),
		Currency:                    validateString(arr[22]),
		AmountatLastUserChange:      validateString(arr[23]),
		CallUnits:                   validateString(arr[24]),
		UnitsatLastUserChange:       validateString(arr[25]),
		CostperUnit:                 validateString(arr[26]),
		MarkUp:                      validateString(arr[27]),
		ExternalTargetingCause:      validateString(arr[28]),
		ExternalTargetedNumber:      validateString(arr[29]),
		CallingPartyServerIpAddress: validateString(arr[30]),
		UniqueCallIDForTheCallerExt: validateString(arr[31]),
		CalledPartyServerIP:         validateString(arr[32]),
		UniqueCallIDforCalledExt:    validateString(arr[33]),
		SMDRRecordTime:              validateDateTime(arr[34]),
		CallerConsentDirective:      validateString(arr[35]),
		CallingNumberVerification:   validateString(arr[36]),
		Undefined:                   validateString(arr[37]),
	}

	return smdr, nil
}

// Expects 00:00:00 input. Hour:Minute:Second
func validateInterval(s string) pgtype.Interval {
	timeArr := strings.Split(s, ":")
	if len(timeArr) != 3 {
		return pgtype.Interval{Valid: false}
	}
	second, err := strconv.ParseInt(timeArr[2], 10, 0)
	if err != nil {
		return pgtype.Interval{Valid: false}
	}

	minute, err := strconv.ParseInt(timeArr[1], 10, 0)
	if err != nil {
		return pgtype.Interval{Valid: false}
	}

	hour, err := strconv.ParseInt(timeArr[0], 10, 0)
	if err != nil {
		return pgtype.Interval{Valid: false}
	}

	var ms int64 = 0
	ms = ms + (second * 1e6)
	ms = ms + (minute * 60e6)
	ms = ms + (hour * 3600e6)

	// not converting days and months since it is VERY unlikley and will have a performance cost.
	return pgtype.Interval{Months: 0, Days: 0, Microseconds: ms, Valid: true}
}

func validateDateTime(s string) sql.NullTime {
	dt, err := time.Parse("2006/01/02 15:04:05", s)
	if err != nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: dt, Valid: true}
}

func validateString(s string) sql.NullString {
	if len(s) < 1 {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}

func validateInt64(s string) sql.NullInt64 {
	i, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: i, Valid: true}
}

func validateBool(s string) sql.NullBool {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return sql.NullBool{Valid: false}
	}
	return sql.NullBool{Bool: b, Valid: true}
}

var pgSchema = `
CREATE TABLE IF NOT EXISTS AvayaData(
	CallStart                   timestamp,
	ConnectedTime               interval,
	RingTime                    integer,
	Caller                      Text,
	CallDirection               Text,
	CalledNumber                Text,
	DialedNumber                Text,
	Account                     Text,
	IsInternal                  boolean,
	CallId                      integer,
	Continuation                boolean,
	Party1Device                Text,
	Party1Name                  Text,
	Party2Device                Text,
	Party2Name                  Text,
	ExternalTargeterId          Text,
	HoldTime                    integer,
	ParkTime                    integer,
	AuthValid                   Text,
	AuthCode                    Text,
	UserCharged                 Text,
	CallCharge                  Text,
	Currency                    Text,
	AmountatLastUserChange      Text,
	CallUnits                   Text,
	UnitsatLastUserChange       Text,
	CostperUnit                 Text,
	MarkUp                      Text,
	ExternalTargetingCause      Text,
	ExternalTargetedNumber      Text,
	CallingPartyServerIpAddress Text,
	UniqueCallIDForTheCallerExt Text,
	CalledPartyServerIP         Text,
	UniqueCallIDforCalledExt    Text,
	SMDRRecordTime              timestamp,
	CallerConsentDirective      Text,
	CallingNumberVerification   Text,
	Undefined                   Text
);
`

// Open connection to PG DB. If connection cannot be opened
// exit fatal to prevent running unregulated.
func pgInit(dbconn string) *sqlx.DB {
	//db, err := sql.Open("pgx", "postgres://pgx_md5:secret@localhost:5432/avaya?sslmode=disable")
	db, err := sqlx.Open("pgx", dbconn)
	if err != nil {
		slog.Error("Failed to open DB",
			slog.String("function", "sqlx.open"),
			slog.Any("err", err))
		log.Fatal("Failed to open DB")
	}

	_, err = db.Exec(pgSchema)
	if err != nil {
		slog.Error("Failed to init schema",
			slog.String("function", "db.Exec"),
			slog.Any("err", err))
		log.Fatal("Failed to init schema")
	}

	return db

}

var insertQuery = `
INSERT INTO	AvayaData (	
CallStart,                 
ConnectedTime,             
RingTime,                  
Caller,                    
CallDirection,             
CalledNumber,              
DialedNumber,              
Account,                   
IsInternal,                
CallId,                    
Continuation,              
Party1Device,              
Party1Name,                
Party2Device,              
Party2Name,                
ExternalTargeterId,        
HoldTime,                  
ParkTime,                  
AuthValid,                 
AuthCode,                  
UserCharged,               
CallCharge,                
Currency,                  
AmountatLastUserChange,    
CallUnits,                 
UnitsatLastUserChange,     
CostperUnit,               
MarkUp,                    
ExternalTargetingCause,    
ExternalTargetedNumber,    
CallingPartyServerIpAddress,
UniqueCallIDForTheCallerExt,
CalledPartyServerIP,       
UniqueCallIDforCalledExt,  
SMDRRecordTime,            
CallerConsentDirective,    
CallingNumberVerification, 
Undefined)

VALUES(
:CallStart,                 
:ConnectedTime,             
:RingTime,                  
:Caller,                    
:CallDirection,             
:CalledNumber,              
:DialedNumber,              
:Account,                   
:IsInternal,                
:CallId,                    
:Continuation,              
:Party1Device,              
:Party1Name,                
:Party2Device,              
:Party2Name,                
:ExternalTargeterId,        
:HoldTime,                  
:ParkTime,                  
:AuthValid,                 
:AuthCode,                  
:UserCharged,               
:CallCharge,                
:Currency,                  
:AmountatLastUserChange,    
:CallUnits,                 
:UnitsatLastUserChange,     
:CostperUnit,               
:MarkUp,                    
:ExternalTargetingCause,    
:ExternalTargetedNumber,    
:CallingPartyServerIpAddress,
:UniqueCallIDForTheCallerExt,
:CalledPartyServerIP,       
:UniqueCallIDforCalledExt,  
:SMDRRecordTime,            
:CallerConsentDirective,    
:CallingNumberVerification, 
:Undefined)
`

func pgInsertSMDR(db *sqlx.DB, smdr SMDR_Packet) {

	defer db.Close()

	_, err := db.NamedQuery(insertQuery, smdr)
	if err != nil {
		slog.Error("Failed to insert",
			slog.String("function", "db.NamedQuery"),
			slog.Any("err", err))
	}

}
