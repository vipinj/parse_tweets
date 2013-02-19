from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Go to http://dev.twitter.com and create an app. 
# The consumer key and secret will be generated for you after
consumer_key="JVMcReNBMQf73696R0lIA"
consumer_secret="NOPvkGZhvvlRWGeRavlY8g043wKB0fRjzY79fnls"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="15106730-pHce180xpivE727KfmRRtumZnB5DjNkXydAg0fkY7"
access_token_secret="jHgNgPCOhCHRNjjsvq6XZM3p7vvQI2CXudYFBdi9jA"

class StdOutListener(StreamListener):
	""" A listener handles tweets are the received from the stream. 
	This is a basic listener that just prints received tweets to stdout.

	"""
	def on_data(self, data):
		f = open("day1_op",'a')
		f.write("%s" %data)
		# print data
		f.close()
		return True

	def on_error(self, status):
		print status

if __name__ == '__main__':
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)

	stream = Stream(auth, l)	
	stream.filter(track=['http'])
