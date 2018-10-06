
# coding: utf-8

# # Predicting Interest rate using Lending Club dataset

# The dataset used is https://www.kaggle.com/wendykan/lending-club-loan-data

# In[1]:


# for numerical analysis and data processing
import numpy as np
import pandas as pd
import itertools


# In[2]:


# for Machine learning algorithms
from sklearn import preprocessing
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE


# In[3]:


# for vizualizations
import matplotlib.pyplot as plt
from matplotlib import cm as cm
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns


# In[4]:


# Reading the files
df = pd.read_csv("loan.csv", low_memory=False)
df_description = pd.read_excel('LCDataDictionary.xlsx').dropna()
df_description.head(20)


# In[5]:


# Analysing the first 5 rows
df.head(5)


# In[6]:


# Here we are just taking the first 9999 rows for analysing
df=df[0:9999]


# In[7]:


# Drop off the columns that are not required
df=df.drop(['pymnt_plan', 'url','initial_list_status', 'last_pymnt_amnt',
            'policy_code','zip_code','id', 'member_id','funded_amnt', 'funded_amnt_inv',
            'emp_title','issue_d','desc','title','earliest_cr_line','mths_since_last_delinq', 'mths_since_last_record','open_acc', 'pub_rec',
            'revol_bal','revol_util','total_acc','out_prncp','out_prncp_inv','total_pymnt','total_pymnt_inv','total_rec_prncp',
            'total_rec_int','total_rec_late_fee','recoveries','collection_recovery_fee', 'last_pymnt_d', 'next_pymnt_d',
       'last_credit_pull_d', 'collections_12_mths_ex_med',
       'mths_since_last_major_derog', 'application_type', 'annual_inc_joint',
       'dti_joint', 'verification_status_joint', 'acc_now_delinq',
       'tot_coll_amt', 'tot_cur_bal', 'open_acc_6m', 'open_il_6m',
       'open_il_12m', 'open_il_24m', 'mths_since_rcnt_il', 'total_bal_il',
       'il_util', 'open_rv_12m', 'open_rv_24m', 'max_bal_bc', 'all_util',
       'total_rev_hi_lim', 'inq_fi', 'total_cu_tl', 'inq_last_12m'], axis=1)


# In[8]:


df.dropna(axis=1,how='all')


# In[9]:


df.info()


# In[10]:


print(df.shape)


# In[11]:


df.columns


# In[12]:


df.loan_status.unique()


# In[13]:


df['loan_status'].value_counts()


# # Feature Engineering:

# In[14]:


df=df.replace({'loan_status':'Fully Paid'},0,regex=True)


# In[15]:


df=df.replace({'loan_status':'Charged Off'},1,regex=True)


# In[16]:


df=df.replace({'loan_status':'Current'},0,regex=True)


# In[17]:


df=df.replace({'loan_status':'Late (31-120 days)'},1)


# In[18]:


df=df.replace({'loan_status':'In Grace Period'},1)


# In[19]:


df=df.replace({'loan_status':'Late (16-30 days)'},1)


# In[20]:


df=df.replace({'loan_status':'Default'},1)


# In[21]:


#Renaming the column 'loan_status' to 'loan_status_Binary'
df = df.rename(columns={'loan_status': 'loan_status_Binary'})


# In[22]:


df['loan_status_Binary'].astype(int)


# In[23]:


df.info()


# In[24]:


df['loan_status_Binary'].value_counts()


# In[25]:


df.head()


# In[26]:


df.info()


# # Exploratory Data Analysis
# In this, we try to look at various statistical and graphical techniques and summarize the relationship between various features of the dataset.

# In[27]:


# To find the correlation between various features, we plot the correlation matrix
def correlation_matrix(df):
    fig = plt.figure(figsize = (10,10))
    ax1 = fig.add_subplot(111)
    cmap = cm.get_cmap('magma')
    corr = df.corr()
    cax = ax1.imshow(corr, interpolation="nearest", cmap=cmap)
    plt.title('Feature Correlation')
    labels = df.columns.values
    print(labels)
    ax1.set_xticklabels(corr.columns,fontsize=10, rotation=90)
    ax1.set_yticklabels(corr.columns,fontsize=10)
    fig.colorbar(cax)
    plt.show()


# In[28]:


correlation_matrix(df.select_dtypes(include=['float64','int64']))
df.dtypes.value_counts().sort_values().plot(kind='bar')
plt.title('Number of features having certain data type')
plt.show()


# In[29]:


numeric_columns = df.select_dtypes(include=['float64','int64']).columns
categorical_columns = df.select_dtypes(include=['object']).columns
for col in categorical_columns:
    df[col] = df[col].astype('category')

categories={}
for cat in categorical_columns:
    categories[cat] = df[cat].cat.categories.tolist()


# In[30]:


p_categories = df['purpose'].cat.categories.tolist()
s_categories = df['addr_state'].cat.categories.tolist()
print(dict( enumerate(df['purpose'].cat.categories) ))


# In[31]:


df[categorical_columns] = df[categorical_columns].apply(lambda x: x.cat.codes)


# In[32]:


# Barplot
sns.barplot(y = categorical_columns, x = df[categorical_columns].apply(pd.Series.nunique, axis = 0).tolist(), palette=sns.color_palette("rainbow"))
plt.title('Number of categories in each categorical feature')
plt.show()


# In[33]:


df.select_dtypes(include=['float64','int64']).describe()


# In[34]:


# Histogram and density chart, to understand the distribution
sns.distplot(df['loan_amnt'], hist=True, kde=True, 
             bins=50, color = 'red', 
             hist_kws={'edgecolor':'black'},
             kde_kws={'linewidth': 4})
plt.xticks(rotation=90)
plt.title('Number of people - Loan Amount')
plt.xlabel('Loan Amount ($)')
plt.ylabel('Number of people')
plt.show()


# In[35]:


print(df['loan_amnt'].describe())


# In[36]:


# Histogram and density chart
sns.distplot(df[df['annual_inc'] <250000]['annual_inc'], hist=True, kde=True, 
             bins=100, color = 'darkblue', 
             hist_kws={'edgecolor':'black'},
             kde_kws={'linewidth': 4})
plt.xticks(rotation=90)
plt.title('Number of people - Annual Income')
plt.xlabel('Annual Income ($)')
plt.ylabel('Number of people')
plt.show()


# In[37]:


print(df['annual_inc'].describe())


# In[38]:


# Bar chart
sns.barplot(y = p_categories, x = df['purpose'].value_counts().sort_index().tolist(), palette=sns.color_palette("copper"))
plt.title('Loan purpose distribution')
plt.show()


# In[39]:


# Summary
print(df['purpose'].describe())


# In[40]:


# Pie Chart
fig1, ax1 = plt.subplots(figsize=(10,10))
ax1.pie(df['purpose'].value_counts().sort_index().tolist(),
        labels=p_categories,
        autopct='%1.1f%%',
        colors= sns.color_palette("copper"))


# In[41]:


ax1.axis('equal')


# In[42]:


centre_circle = plt.Circle((0,0),0.70,fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)


# In[43]:


plt.tight_layout()
plt.show()


# # State wise customer distribution
# The states of Californial has the highest number of borrowers followed by New York and Florida.
# 

# In[44]:


# Bar chart
fig, ax = plt.subplots(figsize=(9,9))
sns.barplot(x=df['addr_state'].value_counts().sort_index().tolist(), y = s_categories, palette=sns.color_palette("RdBu"))
plt.show()


# In[45]:


#Summary
print(df['addr_state'].describe())
# Bar chart
fig, ax = plt.subplots(figsize=(9,9))
sns.barplot(x=df['addr_state'].value_counts().sort_index().tolist(), y = s_categories, palette=sns.color_palette("RdBu"))
plt.show()
get_ipython().show_usage()
#Summary
print(df['addr_state'].describe())


# In[46]:


# Circle chart
fig, ax1 = plt.subplots(figsize=(15,15))
# fig = plt.figure(figsize=(10,10))
ax1.pie(df['addr_state'].value_counts().sort_index().tolist(),
        labels=s_categories,
        autopct='%1.1f%%',
        colors= sns.color_palette("RdBu"))


# In[47]:


ax1.axis('equal')
centre_circle = plt.Circle((0,0),0.70,fc='white')
fig = plt.gcf()


# In[48]:


fig.gca().add_artist(centre_circle)
plt.tight_layout()
plt.show()


# In[49]:


from IPython.display import Image


# In[50]:


# for Machine learning algorithms
from sklearn import preprocessing
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from scipy.spatial.distance import cdist


# In[51]:


# for vizualizations
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
from matplotlib import cm as cm
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns


# In[52]:


min_rate= df['int_rate'].min()
max_rate= df['int_rate'].max()
print(min_rate, max_rate, max_rate- min_rate)
#Storing Interest rate statistics


# # Clustering - k-Means Clustering
# The k-means algorithm searches for a pre-determined number of clusters within an unlabeled multidimensional dataset. According to this algorithm, a simple cluster:
# 
# has a "cluster center" which is the arithmetic mean of all the points belonging to the cluster
# has each point closer to its own cluster center than to other cluster centers
# One of the most important parameters that has to be decided by the user is the value of k, the number of cluster. K random centroids are selected and the centroids are moved with each iteration of the algorithm until all points are assigned a cluster.
# 
# To select the value of K, one of the widely used method is called the Elbow Curve method. Logically, K-Means attempts to minimize distortion defined by the the sum of the squared distances between each observation and its closest centroid. This is called the cost function or distortion. We plot the values of dstortion against K and select where the plot forms an 'elbow joint' i.e. the point after which there is a gradual decrease in the distortion.
# 

# In[53]:


distortions = []
K = range(1,20)
for k in K:
    kmeanModel = KMeans(n_clusters=k).fit(df[0:9999])
    kmeanModel.fit(df[0:9999])
    distortions.append(sum(np.min(cdist(df[0:9999], kmeanModel.cluster_centers_, 'euclidean'), axis=1)) / df[0:9999].shape[0])


# In[54]:


plt.plot(K, distortions, 'bx-')
plt.axvline(5, color='r')
plt.xlabel('k')
plt.ylabel('Distortion')
plt.title('The Elbow Method showing the optimal k')
plt.show()


# The distortion values will eventually reach 0 when the value of K is equal to the number of samples in the dataset, which is same as having one cluster for each point and is undesireable. In this experiment, the distortion changes by 15000 in the first 5 K values and then by 5000 in the next 15 K values. So we select K = 5 as the elbow point.
# 

# In[55]:


num_clusters = 5
num_samples = 9999


# In[56]:


kmeans = KMeans(n_clusters=num_clusters, algorithm='elkan')
kmeans.fit(df[0:num_samples])


# In[57]:


unique, counts = np.unique(kmeans.labels_, return_counts=True)
cluster_indices = np.asarray((unique, counts)).T[:,1].argsort()[-3:][::-1] # change -3 to -5 for all clusters


# In[58]:


print('Samples per cluster:')
print (np.asarray((unique, counts)).T)


# In[59]:


df_cluster = df.join(pd.DataFrame({'cluster': kmeans.labels_}), lsuffix='_caller', rsuffix='_other')


# In[60]:


clusters = []
for i in cluster_indices:
    clusters.append(df_cluster[df_cluster['cluster']==i].loc[:, df.columns != 'cluster'])


# In[61]:


dimensions = 3
# t-SNE parmeters
iterations = 5000
perplexity = 40
# vizualisation is computationally intensive, so we'll stick to 500 samples
num_samples = 500


# In[62]:


# this cell takes a few minutes to execute
tsne = TSNE(n_components=dimensions, random_state=0, perplexity=perplexity, n_iter =iterations)
embeddings = tsne.fit(df[0:num_samples])
# X_2d.embedding_


# In[63]:


embeddings.embedding_[0:5]


# In[64]:


fig = plt.figure(figsize=(10,10))
ax = fig.add_subplot(111, projection='3d')
colors = ['r', 'g', 'b','black', 'c','k']
for i, val in zip(range(len(embeddings.embedding_)),kmeans.labels_):
    plt.scatter(embeddings.embedding_[i,0], embeddings.embedding_[i,1], embeddings.embedding_[i,2], c=colors[val])
plt.show()


# In[65]:


for i, val in zip(range(len(embeddings.embedding_)),kmeans.labels_):
    plt.scatter(embeddings.embedding_[i,0], embeddings.embedding_[i,1],c=colors[val])

