package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/grafana/grafana/pkg/components/null"
	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/tsdb"
	"github.com/pkg/errors"
)

type Clickhouse struct {
	*models.DataSource
	log         log.Logger
	QueryParser *QueryParser
}

type clickhouseResponse struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data []map[string]interface{} 	`json:"data"`
	Rows int64               		`json:"rows"`
}

func NewClickhouseExecutor(dsInfo *models.DataSource) (tsdb.TsdbQueryEndpoint, error) {
	return &Clickhouse{
		DataSource:  dsInfo,
		log:         log.New("tsdb.clickhouse"),
		QueryParser: &QueryParser{},
	}, nil
}

func init() {
	tsdb.RegisterTsdbQueryEndpoint("vertamedia-clickhouse-datasource", NewClickhouseExecutor)
}

func (e *Clickhouse) Query(ctx context.Context, dsInfo *models.DataSource, tsdbQuery *tsdb.TsdbQuery) (*tsdb.Response, error) {
	result := &tsdb.Response{}
	result.Results = make(map[string]*tsdb.QueryResult)

	for _, query := range tsdbQuery.Queries {
		result.Results[query.RefId] = e.executeQuery(query, tsdbQuery.TimeRange)
	}

	return result, nil
}

func (e *Clickhouse) executeQuery(query *tsdb.Query, timeRange *tsdb.TimeRange) *tsdb.QueryResult {
	queryResult := tsdb.NewQueryResult()

	queryString, err := e.QueryParser.Parse(query.Model, timeRange)
	if err != nil {
		e.log.Info(query.Model.String())
		queryResult.Error = errors.Wrap(err, "Cannot get raw query")
		return queryResult
	}
	params := url.Values{}
	params.Add("query", fmt.Sprintf("%s FORMAT JSON", queryString))

	if e.DataSource.BasicAuth {
		params.Add("user", e.DataSource.BasicAuthUser)
		params.Add("password", e.DataSource.DecryptedBasicAuthPassword())
	}

	response, err := http.Get(fmt.Sprintf("%s?%s", e.DataSource.Url, params.Encode()))
	if err != nil {
		queryResult.Error = errors.Wrap(err, "Request is failed")
		return queryResult
	}

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		queryResult.Error = errors.Wrap(err, "Cannot read response body")
		return queryResult
	}

	clickhouseResponse := &clickhouseResponse{}
	err = json.Unmarshal(responseBody, clickhouseResponse)
	if err != nil {
		queryResult.Error = errors.Wrapf(err, "Cannot parse the response: %s", responseBody)
		return queryResult
	}
	format := query.Model.Get("format").MustString("time_series")

	switch format {
	case "time_series":
		series, err := e.buildSeries(clickhouseResponse, timeRange)
		if err != nil {
			queryResult.Error = err
			return queryResult
		}
		queryResult.Series = series
	default:
		queryResult.Error = errors.Errorf("%s format does not support", format)
	}

	return queryResult
}

func (e *Clickhouse) buildSeries(responseJson *clickhouseResponse, timeRange *tsdb.TimeRange) (tsdb.TimeSeriesSlice, error) {
	var series tsdb.TimeSeriesSlice
	points := make(map[string]tsdb.TimeSeriesPoints, 0)

	// time column is always first
	timeColumnName := responseJson.Meta[0].Name

	for _, row := range responseJson.Data {
		timeString := fmt.Sprint(row[timeColumnName])
		time, err := strconv.ParseFloat(timeString, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Cannot parse float %s", timeString))
		}

		if timeRange != nil && (float64(timeRange.GetFromAsMsEpoch()) > time || float64(timeRange.GetToAsMsEpoch()) < time) {
			continue
		}

		// generate series name
		var seriesName string
		stringColumns := make(map[string]bool, 0)
		for _, meta := range responseJson.Meta {
			if meta.Name == timeColumnName {
				continue
			}
			columnValue := fmt.Sprint(row[meta.Name])
			_, err := strconv.ParseFloat(columnValue, 64)
			if err != nil {
				stringColumns[meta.Name] = true
				seriesName += "." + columnValue
			}
		}

		// generate series points
		for columnName, columnValue := range row {
			if columnName == timeColumnName || stringColumns[columnName] {
				continue
			}
			value, err := strconv.ParseFloat(fmt.Sprint(columnValue), 64)
			if err != nil {
				continue
			}

			fullSeriesName := seriesName + "." + columnName
			point := tsdb.NewTimePoint(null.FloatFrom(value), time)
			points[fullSeriesName] = append(points[fullSeriesName], point)
		}
	}

	for metric, values := range points {
		series = append(series, &tsdb.TimeSeries{
			Name:   metric,
			Points: values,
		})
	}

	return series, nil
}
