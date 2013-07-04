#!/usr/bin/env python
# encoding: utf-8

import sys, os, re
import urllib

''' This Python script is for creating a "daily timeline". 
    
    Input: Dump files which are structured as:

    projectcode articlename pagecount sizeofarticle
    Eg: en Barack_Obama 5 56
    Dump files are named according to the date and hour. Eg: pagecounts-20090101-000000.gz
    
    Output: Each article along with the dates, pagecounts and the total pagecount.
    Eg: 'Barack_Obama\t[20090420,20090419]\t[993,1134]\t2127'

    Coded by Sreejith, Nithin - Last modified: April 1, 2012
'''

'''
During execution, the mapper1(), mapper2(), reducer1(), reducer2() are the functions which are called. Note that the functions defined below namely
clean_anchors() and is_valid_title() are used inside these mappers and reducers. They are used to ignore the below in the dump files:
* Pagecount of non english articles
* Pagecounts of pages which aren't articles
* Pagecounts of images.
* Pagecount of Error pages

It can also contain rubbish characters as % and # which are also cleaned. The following regular expressions are used in the cleaning functions.
'''

# Regular expression to exclude pages outside of english wikipedia
wikistats_regex = re.compile('en (.*) ([0-9].*) ([0-9].*)')

# Regular expression to exclude pages outside of namespace 0 (ns0)
namespace_titles_regex = re.compile('(Media|Special' + 
'|Talk|User|User_talk|Project|Project_talk|File' +
'|File_talk|MediaWiki|MediaWiki_talk|Template' +
'|Template_talk|Help|Help_talk|Category' +
'|Category_talk|Portal|Wikipedia|Wikipedia_talk)\:(.*)')

# Regular expressions for excluding articles with first letter lowercase and image files
first_letter_is_lower_regex = re.compile('([a-z])(.*)')
image_file_regex = re.compile('(.*).(jpg|gif|png|JPG|GIF|PNG|txt|ico)')

# Regular expressions to avoid error pages
blacklist = [
'404_error/',
'Main_Page',
'Hypertext_Transfer_Protocol',
'Favicon.ico',
'Search'
]

articledate_regex = re.compile('LongValueSum:(.*)\t([0-9].*)')


# Exclude '#' characters
def clean_anchors(page):
  # pages like Facebook#Website really are "Facebook",
  # ignore/strip anything starting at # from pagename
  anchor = page.find('#')
  if anchor > -1:
    page = page[0:anchor]  # This is called array-slicing in Python. In an array, say page[], page[0:x] returns string from index 0 to x
  return page  


# This function uses the regular expressions defined in the beginning
def is_valid_title(title):
  is_outside_namespace_zero = namespace_titles_regex.match(title)
  if is_outside_namespace_zero is not None:
    return False
  islowercase = first_letter_is_lower_regex.match(title)#someregularexpression.regex.match(something) returns TRUE if someregularexpression matches something
  if islowercase is not None:
    return False
  is_image_file = image_file_regex.match(title)
  if is_image_file:
    return False  
  has_spaces = title.find(' ')
  if has_spaces > -1:
    return False
  if title in blacklist:
    return False   
  return True  

####################################

try:		# Check if we are in a hadoop cluster. This is done by seeing if we're operating in HDFS
  filepath = os.environ["map_input_file"]  #filepath of dumpfile is fetched
  filename = os.path.split(filepath)[-1]   #filename of dumpfile is fetched
except KeyError:
  # use in testing only..
  filename = 'pagecounts-20090419-020000.txt'
      
def mapper1(args):
  '''
  Clean article names, output "en" subset in following format:
  LongValueSum:article}date\tcount
  '''
  # pull the date and time from the filename
  filename_tokens = filename.split('-') 
  ''' A split() function splits a string and returns a list. For eg: s = "good.boy is". split('.') returns ['good',boy is']'''
  (date, time) = filename_tokens[1], filename_tokens[2].split('.')[0] 
  for line in sys.stdin:
      # Read the file, emit 'en' lines with date prepended
      # also un-escape the urls, emit "article}date\t pageviews"
      m = wikistats_regex.match(line)
      if m is not None:
        page, count, bytes = m.groups()
        if is_valid_title(page):
          title = clean_anchors(urllib.unquote_plus(page))
          if len(title) > 0 and title[0] != '#':
            # the following characters are forbidden in wiki page titles
            # so they make good separators: # < > [ ] | { }
            key = '%s}%s' % (title.replace("\t",
            "").replace('}','').replace(' ','_'), date)   
            sys.stdout.write('LongValueSum:%s\t%s\n' % (key, count) )

def reducer1(args):
  '''
  Output as:
  Barack_Obama}20090422  129
  Barack_Obama}20090419  143
  Barack_Obama}20090421  163
  Barack_Obama}20090420  152
  
  '''
  last_articledate, articledate_sum = None, 0
  for line in sys.stdin:
    try:
      match = articledate_regex.match(line)
      articledate, pageviews = match.groups()
      if last_articledate != articledate and last_articledate is not None:
        print '%s\t%s' % (last_articledate, articledate_sum)    
        last_articledate, articledate_sum = None, 0  
      last_articledate = articledate
      articledate_sum += int(pageviews) 
    except:
      # skip bad rows
      pass     
  print '%s\t%s' % (last_articledate, articledate_sum)      
      
      
# Step 2: send rows grouped by article to reducer
def mapper2(args):
  '''    
  Emit "article  date pageview"

  Barack_Obama  20090422 129
  Barack_Obama  20090419 143
  Barack_Obama  20090421 163
  Barack_Obama  20090420 152
  
  '''
  for line in sys.stdin:
    (article_date, pageview) = line.strip().split("\t")
    article, date = article_date.split('}')
    sys.stdout.write('%s\t%s %s\n' % (article, date, pageview))

### A simple trend calculation function is defined below. It is used in reducer2(). This function has not been tested
def calc_trend(dates, pageviews):
  dts,counts = zip( *sorted( zip (dates,pageviews)))
  trend_2 = sum(counts[-15:])
  trend_1 = 1.0*sum(counts[-30:-15])
  monthly_trend = trend_2 - trend_1
  date_str = '[%s]' % ','.join(dts)
  pageview_str = '[%s]' % ','.join(map(str,counts))
  return monthly_trend, date_str, pageview_str


def reducer2(min_days, args):
  '''
  For each article, output a row containing dates, pagecounts, total_pageviews
  Only emit if number of records >= min_days
  Also emit user rating sum and count for use in Hive Join:  (But Hive phase of the project hasn't been started)
  
  Information can be extracted from the output like this:

  line = 'Barack_Obama\t[20090419,20090420,' +
   '20090421,20090422]\t[143,152,163,129]\t587'
  
  >>> line = 'Barack_Obama\t[20090420,20090419]\t[993,1134]\t2127'
  >>> line
  'Barack_Obama\t[20090420,20090419]\t[993,1134]\t2127'
  >>> line.split('\t')
  ['Barack_Obama', '[20090420,20090419]', '[993,1134]', '2127']

  '''
  last_article, dates, pageviews = None, [], []
  total_pageviews = 0
  for line in sys.stdin:
    try:
      (article, date_pageview) = line.strip().split("\t")
      date, pageview = date_pageview.split()
      if last_article != article and last_article is not None:
        if len(dates) >= int(min_days): 
          monthly_trend, date_str, pageview_str = calc_trend(dates, pageviews)  
          sys.stdout.write( "%s\t%s\t%s\t%s\t%s\n" % (last_article, date_str,
           pageview_str, total_pageviews, monthly_trend) )  
        dates, pageviews, total_pageviews = [], [], 0     
      last_article = article
      pageview = int(pageview)
      dates.append(date)
      pageviews.append(pageview)
      total_pageviews += pageview
    except:
      # skip bad rows
      pass  
  # Handle edge case, last row...  
  if len(dates) >= int(min_days):
    monthly_trend, date_str, pageview_str = calc_trend(dates, pageviews)    
    sys.stdout.write( "%s\t%s\t%s\t%s\t%s\n" % (last_article, date_str,
     pageview_str, total_pageviews, monthly_trend) )         
      

#### This piece of code is added for all Python "modules" (not scripts). Basically, adding this allows this module to be used in the Terminal ####      
if __name__ == "__main__":
  if len(sys.argv) == 1:
    print "Running sample data locally..."
    # Step 1
    os.system('cat smallpagecounts-20090419-020000.txt | '+ \
    ' ./daily_pagecounts.py mapper1 | LC_ALL=C sort |' + \
    ' ./daily_pagecounts.py reducer1 > map2_output.txt')
    # Step 2  
    os.system('time cat map2_output.txt |'+ \
    ' ./daily_pagecounts.py mapper2 | LC_ALL=C sort |' + \
    ' ./daily_pagecounts.py reducer2 > part-0000')
    os.system('head part-0000')    
  elif sys.argv[1] == "mapper1":
    mapper1(sys.argv[2:])
  elif sys.argv[1] == "reducer1":
    reducer1(sys.argv[2:])    
  elif sys.argv[1] == "mapper2":
    mapper2(sys.argv[2:])
  elif sys.argv[1] == "reducer2":
    reducer2(sys.argv[2], sys.argv[3:])   
