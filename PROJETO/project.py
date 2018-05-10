
# coding: utf-8

# In[1]:


import re

def removePunctuation(text):
    """Removes punctuation, and strips leading and trailing spaces.

    Note:
        Only letters, and numbers should be retained. 
        Other punctuation will be changed to a simple space.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
    """
    return re.sub(r'[^A-Za-z0-9 ]', ' ', text).strip()


# In[2]:


import os.path

# Read CSV file into a RDD
file = os.path.join('Data', 'ratings_700.csv')
evalsRDD = sc.textFile(file, 4)
    
# Extract header by filtering it out
header = evalsRDD.first()
evalsRDD = evalsRDD.filter(lambda row: row != header)

evalsFormatedRDD = (
    evalsRDD
    .map(removePunctuation)
    .map(lambda x: x.split())
    .map(lambda x: (x[0], (x[1], int(x[2]))))
    .groupByKey(4)
    .mapValues(dict)
)

evalsDict = dict(evalsFormatedRDD.take(1000))
#evalsNumKeys = len(evalsDict)
#print (evalsDict)
#print (evalsNumKeys)


# In[3]:


class RecommendationSystem:

    """
    RecommendationSystem take a dictionary containing user evaluations to items and recommend new items based on
    Weighted SlopeOne Algorithm
    """

    def __init__(self, data):
        """ RecommendationSystem initialization
        :param data: A dictionary containing a user identification and its evaluations (ranging 0-5) to items
        :type data: dict
        """
        self.freqs = {} # Frequencies dict, needed to SlopeOne algorithm
        self.devs = {} # Deviations dict, needed to SlopeOne algorithm
        if type(data).__name__ == 'dict': # Data need to be a dictionary 
            self.data = data

    def calculateDeviations(self):
        """ Deviations calculator: For each user we get their ratings for the items, them analyse each
        one of the ratings and compute the frequencies when they are evaluated together and the 
        deviation between the evaluations
            In the end we iterate through the deviations to divide each by its frequency
        """
        for ratings in self.data.values():
            for (i, first) in ratings.items():
                self.freqs.setdefault(i, {})
                self.devs.setdefault(i, {})
                for (j, second) in ratings.items():
                    if i != j:
                        self.freqs[i].setdefault(j, 0)
                        self.freqs[i][j] += 1
                        self.devs[i].setdefault(j, 0.0)
                        self.devs[i][j] += first - second
                        
        for (i, ratings) in self.devs.items():
            for j in ratings:
                ratings[j] /= self.freqs[i][j]
        
        
    def slopeOne(self, userRatings):
        """
        Slope One Recommender: For every item rated by the user and every item user didnt rate, we calculate the
        possible rate that user could have done to the not rated items by the others ratings and item frequency
        :param userRatings: A dictionary containing all items with evaluations that user had previous evaluated
        :type userRatings: dict
        :return: The recommendation result, a list of tuples 
        """
        recommendations = {}
        freqs = {}

        for (userItem, userRating) in userRatings.items():
            for (diffItem, diffRatings) in self.devs.items():
                if diffItem not in userRatings and userItem in self.devs[diffItem]:
                    auxFreq = self.freqs[diffItem][userItem]
                    recommendations.setdefault(diffItem, 0.0)
                    freqs.setdefault(diffItem, 0)
                    recommendations[diffItem] += (diffRatings[userItem] + userRating) * auxFreq
                    freqs[diffItem] += auxFreq

        recommendations = sc.parallelize([(x, y / freqs[x]) for (x, y) in recommendations.items()])
        recommendationsSorted = recommendations.sortBy(lambda xy: xy[1], False)

        return recommendationsSorted


# In[4]:


recommendation = RecommendationSystem(evalsDict)
recommendation.calculateDeviations()
user = evalsDict['131168']
recommendation.slopeOne(user).take(10)

