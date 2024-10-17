package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	executionTime = flag.String("executionTime", "23:00", "Time to execute the transfer in HH:MM format")
	pgDsn         = flag.String("pgDsn", "", "PostgreSQL DSN")
	mysqlDsn      = flag.String("mysqlDsn", "", "MySQL DSN")
)

func main() {
	flag.Parse()

	for {
		now := time.Now()
		execHour, execMinute := parseExecutionTime(*executionTime)
		execution := time.Date(now.Year(), now.Month(), now.Day(), execHour, execMinute, 0, 0, now.Location())
		if now.After(execution) {
			execution = execution.Add(24 * time.Hour)
		}
		time.Sleep(time.Until(execution))

		transferData(*pgDsn, *mysqlDsn)
	}
}

func parseExecutionTime(timeStr string) (int, int) {
	var hour, minute int
	fmt.Sscanf(timeStr, "%d:%d", &hour, &minute)
	return hour, minute
}

func transferData(pgDsn, mysqlDsn string) {
	// Connect to PostgreSQL
	pgDb, err := sql.Open("postgres", pgDsn)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgDb.Close()

	// Connect to MySQL
	sqlDb, err := sql.Open("mysql", mysqlDsn)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer sqlDb.Close()

	today := time.Now().Format("2006-01-02")

	// 1. Total Machines Count
	machineCount := queryCount(pgDb, `SELECT count(*) FROM machine m WHERE to_timestamp(m.last_commit_solution) >= DATE(NOW())`)
	insertToMySQL(sqlDb, "machines_count", today, machineCount)

	// 2. Active Machines Count
	activeMachineCount := queryCount(pgDb, `SELECT count(*) FROM machine m WHERE to_timestamp(m.last_commit_solution) >= DATE(NOW())`)
	insertToMySQL(sqlDb, "active_machines_count", today, activeMachineCount)

	// 3. Lost Users Count
	lostUsersQuery := `WITH machine_activity AS (
		SELECT ma.main_user_id, MAX(m.last_commit_solution) AS max_last_commit_solution
		FROM miner_account ma
		JOIN machine m ON m.miner_account_id = ma.id
		GROUP BY ma.main_user_id
	)
	SELECT COUNT(u.email) FROM public."user" u
	LEFT JOIN machine_activity ma ON ma.main_user_id = u.id
	WHERE ma.max_last_commit_solution IS NULL
	OR to_timestamp(ma.max_last_commit_solution) < (DATE_TRUNC('day', NOW()) - INTERVAL '2 days')`
	lostUsersCount := queryCount(pgDb, lostUsersQuery)
	insertToMySQL(sqlDb, "lost_users_count", today, lostUsersCount)

	// 4. Active Machines in Channel
	activeMachinesChannelQuery := `WITH select_user AS(
		SELECT u.email, ma.id, ma.name
		FROM miner_account ma
		LEFT JOIN "public"."user" u ON u.id = ma.main_user_id
		LEFT JOIN invitation_code ic ON ic."id" = u.invitation_code_id
		WHERE ic.tag = 'zklion'
	)
	SELECT count(*) FROM machine m 
	JOIN select_user su ON m.miner_account_id = su.id
	WHERE to_timestamp(m.last_commit_solution) >= DATE(NOW())`
	activeMachinesChannelCount := queryCount(pgDb, activeMachinesChannelQuery)
	insertToMySQL(sqlDb, "active_channel_machines_count", today, activeMachinesChannelCount)
}

func queryCount(db *sql.DB, query string) int {
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	return count
}

func insertToMySQL(db *sql.DB, tableName, date string, count int) {
	query := fmt.Sprintf("INSERT INTO %s (date, count) VALUES (?, ?)", tableName)
	_, err := db.Exec(query, date, count)
	if err != nil {
		log.Fatalf("Failed to insert data to MySQL: %v", err)
	}
}