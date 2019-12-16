package clickhouse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana/pkg/components/simplejson"
	"github.com/grafana/grafana/pkg/tsdb"
)

// QueryParser is struct for export parse functions
type QueryParser struct{}

var intervalSteps = map[string]int{
	"s": 1,
	"m": 60,
	"h": 3600,
	"d": 86400,
}

// Parse interpolate query string by data of model and time range
func (qp *QueryParser) Parse(model *simplejson.Json, timeRange *tsdb.TimeRange) (string, error) {
	query, err := model.Get("query").String()
	if err != nil {
		return "", err
	}
	formattedQuery := strings.TrimSpace(query)
	formattedQuery = qp.ParseInterval(formattedQuery, model)
	formattedQuery, err = qp.ParseTimeSeries(formattedQuery, model)
	if err != nil {
		return "", err
	}
	formattedQuery, err = qp.ParseTable(formattedQuery, model)
	if err != nil {
		return "", err
	}
	formattedQuery, err = qp.ParseTimeFilter(formattedQuery, model, timeRange)
	if err != nil {
		return "", err
	}

	reg := regexp.MustCompile(`\$\w*`)
	if reg.MatchString(formattedQuery) {
		return "", fmt.Errorf("Supports in query only $table, $timeSeries, $timeFilter, $interval")
	}

	return formattedQuery, nil
}

// ParseTimeSeries replace $timeSeries to time series of dateTimeColumn from query string
func (qp *QueryParser) ParseTimeSeries(query string, model *simplejson.Json) (string, error) {
	reg := regexp.MustCompile(`\$timeSeries`)
	if !reg.MatchString(query) {
		return query, nil
	}

	dateTimeColumnName, dateTimeType, err := qp.GetDateTimeColumn(model)
	if err != nil {
		return query, err
	}

	var pattern string
	if pattern = "(intDiv(%s, %d) * %d) * 1000"; dateTimeType == "DATETIME" {
		pattern = "(intDiv(toUInt32(%s), %d) * %d) * 1000"
	}
	interval := qp.GetInterval(model)
	timeSeries := fmt.Sprintf(pattern, dateTimeColumnName, interval, interval)
	return reg.ReplaceAllString(query, timeSeries), nil
}

// ParseTable replace $table to names of database and table from query string
func (qp *QueryParser) ParseTable(query string, model *simplejson.Json) (string, error) {
	reg := regexp.MustCompile(`\$table`)
	if !reg.MatchString(query) {
		return query, nil
	}

	table, err := model.Get("table").String()
	if err != nil {
		return query, nil
	}

	database, err := model.Get("database").String()
	if err != nil {
		database = "default"
	}

	return reg.ReplaceAllString(query, fmt.Sprintf("%s.%s", database, table)), nil
}

// ParseInterval replace $interval to calculated interval from data model
func (qp *QueryParser) ParseInterval(query string, model *simplejson.Json) string {
	reg := regexp.MustCompile(`\$interval`)
	if !reg.MatchString(query) {
		return query
	}

	return reg.ReplaceAllString(query, string(qp.GetInterval(model)))
}

// ParseTimeFilter replace $timeFilter to interval of dateTimeColumn by timeRange from query string
func (qp *QueryParser) ParseTimeFilter(query string, model *simplejson.Json, timeRange *tsdb.TimeRange) (string, error) {
	reg := regexp.MustCompile(`\$timeFilter`)
	if !reg.MatchString(query) {
		return query, nil
	}

	dateTimeColumnName, dateTimeType, err := qp.GetDateTimeColumn(model)
	if err != nil {
		return query, err
	}

	from, to := qp.GetTimeRangeAsTimestamps(timeRange, dateTimeType == "DATETIME")
	var result string
	if timeRange.To == "now" {
		result = fmt.Sprintf("%s >= %s", dateTimeColumnName, from)
	} else {
		result = fmt.Sprintf("%s BETWEEN %s AND %s", dateTimeColumnName, from, to)
	}

	return reg.ReplaceAllString(query, result), nil
}

// GetInterval generate interval in seconds for time series by step and interval from data of model
func (qp *QueryParser) GetInterval(model *simplejson.Json) int {
	intervalFactor, err := model.Get("intervalFactor").Int()
	if err != nil {
		intervalFactor = 1
	}

	intervalStr, _ := model.Get("interval").String()

	return intervalFactor * qp.IntervalToSeconds(intervalStr)
}

// IntervalToSeconds convert interval's string to seconds, exp. IntervalToSeconds("5m") => 300
func (qp *QueryParser) IntervalToSeconds(intervalStr string) int {
	if intervalStr == "" {
		return 1
	}

	re := regexp.MustCompile(`^(\d+)(\w+)$`)
	matches := re.FindAllStringSubmatch(intervalStr, -1)
	if len(matches[0]) == 3 {
		value, _ := strconv.Atoi(matches[0][1])
		step := intervalSteps[string(matches[0][2])]
		if value > 0 && step > 0 {
			return value * step
		}
	}

	return 1
}

// GetDateTimeColumn return date or datetime column with date type
func (qp *QueryParser) GetDateTimeColumn(model *simplejson.Json) (string, string, error) {
	dateTimeColumnName, dtErr := model.Get("dateTimeColDataType").String()
	dateColumnName, dErr := model.Get("dateColDataType").String()
	if dtErr != nil && dErr != nil {
		return "", "", dtErr
	}

	dateTimeType, err := model.Get("dateTimeType").String()
	if err != nil {
		dateTimeType = "DATETIME"
	}

	if dateTimeColumnName == "" {
		return dateColumnName, "DATE", nil
	}

	return dateTimeColumnName, dateTimeType, nil
}

// GetTimeRangeAsTimestamps return interval from time range
func (qp *QueryParser) GetTimeRangeAsTimestamps(timeRange *tsdb.TimeRange, isDateTime bool) (string, string) {
	now := time.Now().Unix()
	from := now - int64(qp.IntervalToSeconds(timeRange.From))

	matches := strings.Split(timeRange.To, "-")
	var to int64
	if to = now; len(matches) > 1 {
		to -= int64(qp.IntervalToSeconds(timeRange.To))
	}

	var pattern string
	if pattern = "%d"; !isDateTime {
		pattern = "toDate(%d)"
	}

	return fmt.Sprintf(pattern, from), fmt.Sprintf(pattern, to)
}
