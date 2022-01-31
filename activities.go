package activities

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq" //for sql
	"github.com/sony/gobreaker"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

type Activities struct {
	Name     string
	Postcode string
	Sunny    bool
}

type Handler struct {
	Logger         log.Logger
	Db             sql.DB
	Redis          redis.Client
	CircuitBreaker *gobreaker.CircuitBreaker
}

type Weather struct {
	Coord struct {
		Lon float64 `json:"lon"`
		Lat float64 `json:"lat"`
	} `json:"coord"`
	Weather []struct {
		ID          int    `json:"id"`
		Main        string `json:"main"`
		Description string `json:"description"`
		Icon        string `json:"icon"`
	} `json:"weather"`
	Base string `json:"base"`
	Main struct {
		Temp      float64 `json:"temp"`
		FeelsLike float64 `json:"feels_like"`
		TempMin   float64 `json:"temp_min"`
		TempMax   float64 `json:"temp_max"`
		Pressure  int     `json:"pressure"`
		Humidity  int     `json:"humidity"`
		SeaLevel  int     `json:"sea_level"`
		GrndLevel int     `json:"grnd_level"`
	} `json:"main"`
	Visibility int `json:"visibility"`
	Wind       struct {
		Speed float64 `json:"speed"`
		Deg   int     `json:"deg"`
		Gust  float64 `json:"gust"`
	} `json:"wind"`
	Clouds struct {
		All int `json:"all"`
	} `json:"clouds"`
	Dt  int `json:"dt"`
	Sys struct {
		Type    int    `json:"type"`
		ID      int    `json:"id"`
		Country string `json:"country"`
		Sunrise int    `json:"sunrise"`
		Sunset  int    `json:"sunset"`
	} `json:"sys"`
	Timezone int    `json:"timezone"`
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Cod      int    `json:"cod"`
}

var ActivitiesList = []Activities{
	{"shopping belfast", "Belfast", true},
	{"mini golf", "Craigavon", true},
	{"shopping dublin", "Dublin", true},
	{"cinema", "Lisburn", false},
	{"axe throwing", "Belfast", false},
	{"swimming", "Armagh", false},
	{"Walk around Stormount", "Belfast", false},
	{"Hike Cavehill", "Belfast", true},
	{"Trip to the causeway coast", "Coleraine", true},
	{"Walk around castlewellan", "Castlewellan", true},
	{"Gosford Forest Park", "Armagh", true},
	{"Go to the Ulster folk and Transport Museum", "Omagh", false},
	{"Slieve Guillion Trip", "Armagh", true},
	{"Marble Arch Caves", "Fermanagh", true},
	{"Cuilcagh (Stairway to Heaven)", "Fermanagh", true},
}

func (h *Handler) SunnyEndpoint() func(writer http.ResponseWriter, request *http.Request) {
	//h.db.QueryContext()
	return func(writer http.ResponseWriter, request *http.Request) {
		startTime := time.Now()
		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte(h.getSunnyActivity(request.Context())))
		if err != nil {
			log.Fatalln("could not write the bytes")
		}
		fmt.Println(time.Since(startTime))
	}
}

func (h *Handler) getSunnyActivity(ctx context.Context) string {
	var a Activities
	var newActivityList []Activities

	sunnyActivitiesQuery := "SELECT * FROM activities where sunny = $1"
	rows, err := h.Db.Query(sunnyActivitiesQuery, true)
	if err != nil {
		log.Fatalln("An error occurred", err)
	}
	for rows.Next() {
		err = rows.Scan(&a.Name, &a.Postcode, &a.Sunny)
		if err != nil {
			log.Fatalln("Error in scanning db rows", err)
		}
		newActivityList = append(newActivityList, a)
	}
	var discardedActivityList []Activities
	choosenActivity, _ := h.retrieveActivity(ctx, newActivityList, discardedActivityList, true)
	return fmt.Sprintf("%s %s", choosenActivity.Name, choosenActivity.Postcode)
}

func (h *Handler) retrieveActivity(ctx context.Context, newActivityList []Activities, discardedActivityList []Activities, sunny bool) (Activities, error) {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	randomNumber := r1.Intn(len(newActivityList))
	choosenActivity := newActivityList[randomNumber]
	if sunny {
		//check cache
		value, err := h.Redis.Get(ctx, choosenActivity.Postcode).Result()
		if err == redis.Nil {
			// we want to call the API
			weather := choosenActivity.GetWeather()

			err = h.Redis.Set(ctx, choosenActivity.Postcode, weather, time.Minute*10).Err()

			if weather != "Rain" && weather != "Snow" && weather != "Drizzle" {
				return choosenActivity, nil
			} else {
				discardedActivityList = append(discardedActivityList, choosenActivity)
				newActivityList = h.RemoveIndex(newActivityList, randomNumber)
				return h.retrieveActivity(ctx, newActivityList, discardedActivityList, true)
			}
		} else if err != nil {
			return Activities{}, err
		} else {
			// build response
			if value != "Rain" && value != "Snow" && value != "Drizzle" {
				return choosenActivity, nil
			} else {
				discardedActivityList = append(discardedActivityList, choosenActivity)
				newActivityList = h.RemoveIndex(newActivityList, randomNumber)
				return h.retrieveActivity(ctx, newActivityList, discardedActivityList, true)
			}
		}

	} else {
		return choosenActivity, nil
	}
	return Activities{}, nil
}

func (a *Activities) GetWeather() string {

	url := fmt.Sprintf("http://api.openweathermap.org/data/2.5/weather?appid=%s&q=%s", os.Getenv("WEATHER_API_KEY"), a.Postcode)
	response, err := http.Get(url)
	if err != nil {
		log.Fatalln("retrieving the weather", err)
	}
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalln("retriving the body", err)
	}
	var weather Weather

	if err := json.Unmarshal(body, &weather); err != nil {
		log.Fatalln("error unmarshalling response to json", err)
	}
	return weather.Weather[0].Main

}

func (h *Handler) NotSunnyEndpoint() func(writer http.ResponseWriter, request *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		startTime := time.Now()
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte(h.getNotSunnyActivities(request.Context()))) //nolint:errcheck
		fmt.Println(time.Since(startTime))
	}
}

func (h *Handler) getNotSunnyActivities(ctx context.Context) string {

	var a Activities
	var newActivityList []Activities

	notSunnyActivitiesQuery := "SELECT * FROM activities where sunny = $1"
	rows, err := h.Db.Query(notSunnyActivitiesQuery, false)
	if err != nil {
		log.Fatalln("An error occurred", err)
	}
	for rows.Next() {
		err = rows.Scan(&a.Name, &a.Postcode, &a.Sunny)
		if err != nil {
			log.Fatalln("Error in scanning db rows", err)
		}
		newActivityList = append(newActivityList, a)
	}
	var discardedActivityList []Activities
	choosenActivity, _ := h.retrieveActivity(ctx, newActivityList, discardedActivityList, false)
	return fmt.Sprintf("%s %s", choosenActivity.Name, choosenActivity.Postcode)

	//value, err := h.Redis.Get(ctx, "allWeatherActivities").Result()
	//if err == redis.Nil {
	//	var a Activities
	//	var newActivityList []Activities
	//
	//	notSunnyActivitiesQuery := "SELECT * FROM activities where sunny = $1"
	//	rows, err := h.Db.Query(notSunnyActivitiesQuery, false)
	//	if err != nil {
	//		log.Fatalln("An error occurred", err)
	//	}
	//	for rows.Next() {
	//		err = rows.Scan(&a.Name, &a.Postcode, &a.Sunny)
	//		if err != nil {
	//			log.Fatalln("Error in scanning db rows", err)
	//		}
	//		newActivityList = append(newActivityList, a)
	//	}
	//
	//	h.Redis.Set(ctx, "allWeatherActivites", newActivityList, time.Minute*15)
	//	var discardedActivityList []Activities
	//	choosenActivity, _ := h.retrieveActivity(ctx, newActivityList, discardedActivityList, false)
	//	return fmt.Sprintf("%s %s", choosenActivity.Name, choosenActivity.Postcode), nil
	//} else if err != nil {
	//	return "error", err
	//} else {
	//	// build response
	//	var discardedActivityList []Activities
	//	choosenActivity, _ := h.retrieveActivity(ctx, value, discardedActivityList, false)
	//	return fmt.Sprintf("%s %s", choosenActivity.Name, choosenActivity.Postcode), nil
	//	return value, nil
	//}
}

func (h *Handler) RemoveIndex(s []Activities, index int) []Activities {
	return append(s[:index], s[index+1:]...)
}
