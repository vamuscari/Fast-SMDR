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

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

var logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

func main() {
	var port int = 514
	var err error
	var filter net.IP

	// Input Args
	for i, v := range os.Args {
		switch v {
		// Port
		case "-p":
			port, err = strconv.Atoi(os.Args[(i + 1)])
			if err != nil {
				log.Fatalln(err)
			}
			// filter address
		case "-f":
			filter = net.ParseIP(os.Args[(i + 1)])
		}
	}

	ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalln(err)
		return
	}

	slog.SetDefault(logger)
	slog.Info(fmt.Sprintf("Listening on %s\n", ln.Addr().String()))

	pgInit()

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("Failed to Accept Incoming", slog.Any("err", err))
			continue
		}

		if checkConnection(conn, filter) {
			go handleConnection(conn)
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

func handleConnection(conn net.Conn) {
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

	pgInsertSMDR(smdr)

}

type SMDR_Packet struct {
	CallStart                   sql.NullTime   `db:"CallStart"`
	ConnectedTime               sql.NullTime   `db:"ConnectedTime"`
	RingTime                    sql.NullInt64  `db:"RingTime"`
	Caller                      sql.NullString `db:"Caller"`
	CallDirection               sql.NullString `db:"CallDirection"`
	CalledNumber                sql.NullString `db:"CalledNumber"`
	DialedNumber                sql.NullString `db:"DialedNumber"`
	Account                     sql.NullString `db:"Account"`
	IsInternal                  sql.NullBool   `db:"IsInternal"`
	CallId                      sql.NullInt64  `db:"CallId"`
	Continuation                sql.NullBool   `db:"Continuation"`
	Party1Device                sql.NullString `db:"Party1Device"`
	Party1Name                  sql.NullString `db:"Party1Name"`
	Party2Device                sql.NullString `db:"Party2Device"`
	Party2Name                  sql.NullString `db:"Party2Name"`
	ExternalTargeterId          sql.NullString `db:"ExternalTargeterId"`
	HoldTime                    sql.NullInt64  `db:"HoldTime"`
	ParkTime                    sql.NullInt64  `db:"ParkTime"`
	AuthValid                   sql.NullString `db:"AuthValid"`
	AuthCode                    sql.NullString `db:"AuthCode"`
	UserCharged                 sql.NullString `db:"UserCharged"`
	CallCharge                  sql.NullString `db:"CallCharge"`
	Currency                    sql.NullString `db:"Currency"`
	AmountatLastUserChange      sql.NullString `db:"AmountatLastUserChange"`
	CallUnits                   sql.NullString `db:"CallUnits"`
	UnitsatLastUserChange       sql.NullString `db:"UnitsatLastUserChange"`
	CostperUnit                 sql.NullString `db:"CostperUnit"`
	MarkUp                      sql.NullString `db:"MarkUp"`
	ExternalTargetingCause      sql.NullString `db:"ExternalTargetingCause"`
	ExternalTargetedNumber      sql.NullString `db:"ExternalTargetedNumber"`
	CallingPartyServerIpAddress sql.NullString `db:"CallingPartyServerIpAddress"`
	UniqueCallIDForTheCallerExt sql.NullString `db:"UniqueCallIDForTheCallerExt"`
	CalledPartyServerIP         sql.NullString `db:"CalledPartyServerIP"`
	UniqueCallIDforCalledExt    sql.NullString `db:"UniqueCallIDforCalledExt"`
	SMDRRecordTime              sql.NullTime   `db:"SMDRRecordTime"`
	CallerConsentDirective      sql.NullString `db:"CallerConsentDirective"`
	CallingNumberVerification   sql.NullString `db:"CallingNumberVerification"`
	Undefined                   sql.NullString `db:"Undefined"`
}

func parseBuffer(buf []byte) (SMDR_Packet, error) {

	var smdr SMDR_Packet

	arr := strings.Split(fmt.Sprintf("%s", buf), ",")
	if len(arr) != 38 {
		for i, v := range arr {
			fmt.Printf("%d: %s\n", (i + 1), v)
		}
		return smdr, errors.New(fmt.Sprintf("parseBuffer: %d columns received instead of 38", len(arr)))
	}

	smdr.CallStart = validateDateTime(arr[0])
	smdr.ConnectedTime = validateTime(arr[1])
	smdr.RingTime = validateInt64(arr[2])
	smdr.Caller = validateString(arr[3])
	smdr.CallDirection = validateString(arr[4])
	smdr.CalledNumber = validateString(arr[5])
	smdr.DialedNumber = validateString(arr[6])
	smdr.Account = validateString(arr[7])
	smdr.IsInternal = validateBool(arr[8])
	smdr.CallId = validateInt64(arr[9])
	smdr.Continuation = validateBool(arr[10])
	smdr.Party1Device = validateString(arr[11])
	smdr.Party1Name = validateString(arr[12])
	smdr.Party2Device = validateString(arr[13])
	smdr.Party2Name = validateString(arr[14])
	smdr.ExternalTargeterId = validateString(arr[15])
	smdr.HoldTime = validateInt64(arr[16])
	smdr.ParkTime = validateInt64(arr[17])
	smdr.AuthValid = validateString(arr[18])
	smdr.AuthCode = validateString(arr[19])
	smdr.UserCharged = validateString(arr[20])
	smdr.CallCharge = validateString(arr[21])
	smdr.Currency = validateString(arr[22])
	smdr.AmountatLastUserChange = validateString(arr[23])
	smdr.CallUnits = validateString(arr[24])
	smdr.UnitsatLastUserChange = validateString(arr[25])
	smdr.CostperUnit = validateString(arr[26])
	smdr.MarkUp = validateString(arr[27])
	smdr.ExternalTargetingCause = validateString(arr[28])
	smdr.ExternalTargetedNumber = validateString(arr[29])
	smdr.CallingPartyServerIpAddress = validateString(arr[30])
	smdr.UniqueCallIDForTheCallerExt = validateString(arr[31])
	smdr.CalledPartyServerIP = validateString(arr[32])
	smdr.UniqueCallIDforCalledExt = validateString(arr[33])
	smdr.SMDRRecordTime = validateDateTime(arr[34])
	smdr.CallerConsentDirective = validateString(arr[35])
	smdr.CallingNumberVerification = validateString(arr[36])
	smdr.Undefined = validateString(arr[37])

	return smdr, nil
}

func validateDateTime(s string) sql.NullTime {
	dt, err := time.Parse(time.DateTime, s)
	if err != nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: dt, Valid: true}
}

func validateTime(s string) sql.NullTime {
	t, err := time.Parse(time.TimeOnly, s)
	if err != nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: t, Valid: true}
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
	CallStart                   timestamp
	ConnectedTime               interval
	RingTime                    integer
	Caller                      Text
	CallDirection               Text
	CalledNumber                Text
	DialedNumber                Text
	Account                     Text
	IsInternal                  boolean
	CallId                      integer
	Continuation                boolean
	Party1Device                Text
	Party1Name                  Text
	Party2Device                Text
	Party2Name                  Text
	ExternalTargeterId          Text
	HoldTime                    integer
	ParkTime                    integer
	AuthValid                   Text
	AuthCode                    Text
	UserCharged                 Text
	CallCharge                  Text
	Currency                    Text
	AmountatLastUserChange      Text
	CallUnits                   Text
	UnitsatLastUserChange       Text
	CostperUnit                 Text
	MarkUp                      Text
	ExternalTargetingCause      Text
	ExternalTargetedNumber      Text
	CallingPartyServerIpAddress Text
	UniqueCallIDForTheCallerExt Text
	CalledPartyServerIP         Text
	UniqueCallIDforCalledExt    Text
	SMDRRecordTime              timestamp
	CallerConsentDirective      Text
	CallingNumberVerification   Text
	Undefined                   Text
);
`

func pgInit() {
	db, err := sql.Open("pgx", "postgres://pgx_md5:secret@localhost:5432/avaya?sslmode=disable")
	if err != nil {
		log.Fatal("Failed to init postgres DB")
	}

	db.Exec(pgSchema)

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
CallingPartyServerIpAddress
UniqueCallIDForTheCallerExt
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
:CallingPartyServerIpAddress
:UniqueCallIDForTheCallerExt
:CalledPartyServerIP,       
:UniqueCallIDforCalledExt,  
:SMDRRecordTime,            
:CallerConsentDirective,    
:CallingNumberVerification, 
:Undefined)
`

func pgInsertSMDR(smdr SMDR_Packet) {

	db, err := sqlx.Open("pgx", "postgres://pgx_md5:secret@localhost:5432/avaya?sslmode=disable")
	if err != nil {
		slog.Error("Failed to insert",
			slog.String("function", "sqlx.open"),
			slog.Any("err", err))
	}

	_, err = db.NamedQuery(insertQuery, smdr)
	if err != nil {
		slog.Error("Failed to insert",
			slog.String("function", "db.NamedQuery"),
			slog.Any("err", err))
	}
}
