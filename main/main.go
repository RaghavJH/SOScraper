package main

import (
	"fmt"
	"github.com/gocolly/colly"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

/* User struct */
type User struct {
	Name       string
	Location   string
	Reputation int32
	Skills     [3]string
}

func (u *User) String() string {
	return fmt.Sprintf(
		"Name: %s\nLocation: %s\nReputation: %d\nSkill1: %s, Skill2: %s, Skill3: %s",
		u.Name, u.Location, u.Reputation, u.Skills[0], u.Skills[1], u.Skills[2],
	)
}

func main() {
	//Create colly
	c := colly.NewCollector(
		colly.Async(),
		colly.AllowURLRevisit(),
	)

	//Set parallelism rule
	err := c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 50})
	if err != nil {
		log.Fatalf("Failed to set parallelism options: %s", err)
	}
	/* Now we get the number of pages of users */
	fmt.Println("Scraping max pages...")
	maxPages, err := GetMaxPages(c)
	if err != nil {
		log.Fatalf("Failed to get max pages: %s", err)
	}
	fmt.Println("Finished scraping max pages")
	/* Get users */
	fmt.Println("Scraping users...")
	users, err := ScrapeUsers(c, maxPages)
	if err != nil {
		log.Fatalf("Failed to get users: %s", err)
	}
	fmt.Println("Finished scraping users")
	/* Write to CSV */
	fmt.Println("Writing to CSV...")
	err = WriteToCSV(users, "data.csv")
	if err != nil {
		log.Fatalf("Error creating CSV file: %s", err)
	}
	fmt.Println("Finished writing to CSV")
}

func WriteToCSV(users []*User, filepath string) (err error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	/* Write table headers */
	if _, err = f.WriteString("Id,Name,Location,Reputation,Skill1,Skill2,Skill3\n"); err != nil {
		f.Close() //Ignore
		return
	}
	/* Write data points to table */
	for i, user := range users {
		toWrite := fmt.Sprintf(
			"%d,\"%s\",\"%s\",%d,\"%s\",\"%s\",\"%s\"\n",
			i+1, user.Name, user.Location, user.Reputation, user.Skills[0], user.Skills[1], user.Skills[2],
		)
		if _, err = f.WriteString(toWrite); err != nil {
			f.Close()
			return
		}
	}

	err = f.Close()

	return
}

func ScrapeUsers(c *colly.Collector, maxPages int64) (users []*User, err error) {
	//Scrape maximum 1 million users, minimum whatever the minimum is
	//36 users per page
	maxPages = int64(math.Min(float64(maxPages), math.Ceil(1000000/36.0)))
	users = make([]*User, 0, maxPages*36)
	//Mutex for safely appending to users array
	var mux sync.Mutex
	//Printing to track progress
	var muxPrint sync.Mutex
	scraped := int64(0)

	c.OnHTML(".user-info", func(e *colly.HTMLElement) {
		user := &User{}
		//For each div within the user-info div
		e.ForEach("div", func(_ int, child *colly.HTMLElement) {
			//Switch div class to get different info
			switch child.Attr("class") {
			case "user-details":
				user.Name = child.ChildText("a")
				user.Location = child.ChildText(".user-location")
				break
			case "-flair":
				reputationStr := child.ChildText(".reputation-score")
				//Remove commas from the number e.g. 9,365 becomes 9365
				reputationStr = strings.ReplaceAll(reputationStr, ",", "")
				var reputation int32
				if strings.HasSuffix(reputationStr, "k") {
					//Remove the k at the end
					reputationStr = reputationStr[0 : len(reputationStr)-1]
					//Float because it can be 9.7k for instance, but also can be normal e.g. 100k
					temp, _ := strconv.ParseFloat(reputationStr, 32)
					temp *= 1000.0 //Account for the "k" at the end
					reputation = int32(temp)
				} else {
					//Less than 1000 rep, no need to parse "k"
					temp, _ := strconv.ParseInt(reputationStr, 10, 32)
					reputation = int32(temp)
				}
				user.Reputation = reputation
				break
			case "user-tags":
				//Each skill is stored in an a tag e.g. <a href="...">skill_here</a>
				child.ForEach("a", func(i int, grandchild *colly.HTMLElement) {
					//Not really needed, just for safety in case
					//SO ever decides to add more than just top 3
					if i < len(user.Skills) {
						user.Skills[i] = grandchild.Text
					}

				})
				break
			}
		})

		//Safely append user to array
		mux.Lock()
		users = append(users, user)
		mux.Unlock()
		//Safely print
		muxPrint.Lock()
		scraped += 1
		fmt.Printf("Scraped %d users\n", scraped)
		muxPrint.Unlock()
	})

	//Scrape each page in its own Goroutine (max 50 at a time as set in main func)
	for page := int64(1); page <= maxPages; page++ {
		err = c.Visit(
			fmt.Sprintf("https://stackoverflow.com/users?page=%d&tab=Reputation&filter=month", page),
		)
		//Perhaps remove this?
		//If a single page fails to scrape, ignore it and still scrape the others
		if err != nil {
			return
		}
	}

	//Wait for scraping to finish
	c.Wait()

	return
}

func GetMaxPages(c *colly.Collector) (int64, error) {
	maxPages := int64(1)
	c.OnHTML(".s-pagination", func(e *colly.HTMLElement) {
		e.ForEach(".s-pagination--item", func(_ int, child *colly.HTMLElement) {
			//If it is a valid integer... (to avoid stuff like "..." and "Next")
			if num, err := strconv.ParseInt(child.Text, 10, 64); err == nil {
				if num > maxPages {
					maxPages = num
				}
			}
		})
	})
	/* Visit URL to get max pages */
	err := c.Visit("https://stackoverflow.com/users")
	/* Wait for completion */
	c.Wait()
	return maxPages, err
}
