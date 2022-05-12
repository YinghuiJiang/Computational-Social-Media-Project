# Code largely taken from: https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a

# For sending GET requests from the API
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
# For dealing with json responses we receive from the API
import json
# For displaying the data after
import pandas as pd
# For saving the response data in CSV format
import csv
# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
# To add wait time between requests
import time

os.environ['TOKEN'] = 'AAAAAAAAAAAAAAAAAAAAAJ6YawEAAAAAmKIXPNxPqYTRT1YNT3n3uRJXPyc%3DIol73GbIHjudBkiRI3hjtsLOiKXe8pogfFXUa9A4qv6TlXdhsz'


def auth():
    return os.getenv('TOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results=10):
    # Change to the endpoint you want to collect data from
    search_url = "https://api.twitter.com/2/tweets/search/all"

    # change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}

    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token=None):
    # params object received from create_url function
    params['next_token'] = next_token
    response = requests.request("GET", url, headers=headers, params=params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def append_to_csv(json_response, fileName):
    # A counter variable
    counter = 0

    # Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)
    print("json_response type: ", type(json_response))
    # Loop through each tweet
    print("json_response data type: ", type(json_response['data']))
    print("json_response includes type: ", type(json_response['includes']))
    includes = json_response['includes']
    for tweet in json_response['data']:
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

        # 1. Author ID
        author_id = tweet['author_id']

        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])

        # 3. Geolocation
        if ('geo' in tweet):
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        # 4. Tweet ID
        tweet_id = tweet['id']

        # 5. Language
        lang = tweet['lang']

        # 6. Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']

        # 7. source
        source = tweet['source']

        # 8. Tweet text
        text = tweet['text']
        
        # 9. Username and user public metric
        users = includes['users']
        for user in users:
            is_verified_user = user['verified']
            if user['id'] == author_id:
                username = user['username']
                print("username: ", username)
                
                user_public_metrics = user['public_metrics']
                followers_count = user_public_metrics['followers_count']
                following_count = user_public_metrics['following_count']
                tweet_count = user_public_metrics['tweet_count']
                listed_count = user_public_metrics['listed_count']

        # Assemble all data in a list
        res = [author_id, username, is_verified_user, followers_count, following_count, tweet_count, listed_count, created_at, geo, tweet_id, lang, like_count,
               quote_count, reply_count, retweet_count, source, text]

        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()

    # Print the number of tweets for this iteration
    print("# of Tweets added from this response: ", counter)


# Inputs for tweets
bearer_token = auth()
headers = create_headers(bearer_token)

# *************************************** EDIT this section ************************************************
language = 'en'
keyword = f'EUR \\"and\\"  CHF lang:{language} -is:retweet'
start_list = ['2012-07-26T00:00:00.000Z',
              '2012-08-26T00:00:00.000Z',
              '2012-09-26T00:00:00.000Z',
              '2012-10-26T00:00:00.000Z',
              '2012-11-26T00:00:00.000Z'] #2012-07-25T23:59:59.000Z

# Add intervals of months INTO the list as such: '2012-09-26T00:00:00.000Z', '2012-10-26T00:00:00.000Z'
end_list = ['2012-08-25T23:59:59.000Z', 
            '2012-09-25T23:59:59.000Z', 
            '2012-10-25T23:59:59.000Z', 
            '2012-11-25T23:59:59.000Z', 
            '2012-12-25T23:59:59.000Z']
max_results = 10  # 10 is for testing only. NOTE: minimum value is 10

# *********************************************************************************************************

# Total number of tweets we collected from the loop
total_tweets = 0

for i in range(0, len(start_list)):

    # Inputs
    count = 0  # Counting tweets per time period
    max_count = 10  # Max tweets per time period
    flag = True
    next_token = None

    # Create file
    csvFile = open(f"data\data_{start_list[i][:10]}_{language}.csv", "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # Create headers for the data you want to save, in this example, we only want save these columns in our dataset
    csvWriter.writerow(['author id','author username', 'is_verified_user', 'followers_count', 'following_count', 'tweet_count', 'listed_count', 'created_at', 'geo', 'id', 'lang', 'like_count',
                    'quote_count', 'reply_count', 'retweet_count', 'source', 'tweet'])
    csvFile.close()

    # Check if flag is true
    while flag:
        # Check if max_count reached
        if count >= max_count:
            break
        print("-------------------")
        print("Token: ", next_token)
        url = create_url(keyword, start_list[i], end_list[i], max_results)
        print("url: ",url)
        json_response = connect_to_endpoint(
            url[0], headers, url[1], next_token)
        # print(json_response)
        result_count = json_response['meta']['result_count']

        if 'next_token' in json_response['meta']:
            # Save the token to use for next call
            next_token = json_response['meta']['next_token']
            print("Next Token: ", next_token)
            if result_count is not None and result_count > 0 and next_token is not None:
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, f"data\data_{start_list[i][:10]}_{language}.csv")
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("-------------------")
                time.sleep(5)
        # If no next token exists
        else:
            if result_count is not None and result_count > 0:
                print("-------------------")
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, f"data\data_{language}.csv")
                count += result_count
                total_tweets += result_count
                print("Total # of Tweets added: ", total_tweets)
                print("-------------------")
                time.sleep(5)

            # Since this is the final request, turn flag to false to move to the next time period.
            flag = False
            next_token = None
        time.sleep(5)
print("Total number of results: ", total_tweets)
