
# coding: utf-8

# In[8]:


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


# In[33]:


import os.path

# Read CSV file into a RDD
file = os.path.join('Data', 'evals_table.csv')
evalsRDD = sc.textFile(file, 4)

# Extract header by filtering it out
header = evalsRDD.first()
evalsRDD = evalsRDD.filter(lambda row: row != header)

evalsFormatedRDD = (
                    evalsRDD
                     .map(removePunctuation)
                     .map(lambda x: x.split())
                     .map(lambda x: (x[0], (x[1], x[2])))
                     .groupByKey(4)
                     .mapValues(dict)
                   )

evalsDict = dict(evalsFormatedRDD.collect())
evalsNumKeys = len(evalsDict)
#print (evalsDict)
#Sprint (evalsNumKeys)

