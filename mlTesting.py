import pandas as pd
import numpy as np
from sklearn import preprocessing
import os
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.metrics import jaccard_similarity_score
from sklearn.metrics import log_loss
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import brier_score_loss
from sklearn import model_selection
from sklearn.model_selection import cross_val_predict
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import roc_auc_score
import pickle


engine = create_engine('postgresql://kylelane@localhost:5432/kylelane')
sql = """SELECT * from  nhlstats.allplays 
    WHERE event_type = 'Shot' OR event_type = 'Goal' OR event_type ='Missed Shot'
    """

allShots = pd.read_sql_query(sql, con=engine)

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)

print(allShots.head())
print(allShots.head)
test = allShots.event_desc.unique()
# testing = allShots[allShots['event_desc'] is None]
# print(testing.head)

# net center location
goal_x = 89
goal_y = 0

allShots['adjx'] = abs(allShots['x_coords'])
allShots['adjy'] = abs(allShots['y_coords'])

allShots['goal_binary'] = np.where(allShots['event_type'] == 'Goal', 1, 0)

allShots['wrist_shot'] = np.where(allShots['event_desc'] == 'Wrist Shot', 1, 0)
allShots['backhand'] = np.where(allShots['event_desc'] == 'Backhand', 1, 0)
allShots['slap_shot'] = np.where(allShots['event_desc'] == 'Slap Shot', 1, 0)
allShots['snap_shot'] = np.where(allShots['event_desc'] == 'Snap Shot', 1, 0)
allShots['tip_in'] = np.where(allShots['event_desc'] == 'Tip-In', 1, 0)
allShots['deflected'] = np.where(allShots['event_desc'] == 'Deflected', 1, 0)
allShots['wrap_around'] = np.where(allShots['event_desc'] == 'Wrap-around', 1, 0)
allShots['none'] = np.where(allShots['event_desc'] == 'None', 1, 0)

allShots = allShots[allShots['event_type_id'] != 'MISSED_SHOT']

# Distance and angle of location
allShots['dist'] = np.sqrt(np.power((allShots['adjx'] - goal_x), 2) + np.power((allShots['y_coords'] - goal_y), 2))
allShots['ang'] = 180 - (np.arctan2(allShots['adjy'], allShots['adjx']-100) * 180 / np.pi)

print(allShots.shape)

allShots.dropna()
allShots = allShots[np.isfinite(allShots['y_coords'])]
allShots = allShots[np.isfinite(allShots['x_coords'])]

# X = np.asarray(allShots[['dist',
#                          'ang',
#                          'adjx',
#                          'adjy',
#                          'wrist_shot',
#                          'backhand',
#                          'snap_shot',
#                          'slap_shot',
#                          'tip_in',
#                          'deflected',
#                          'wrap_around'
#                          ]])
X = np.asarray(allShots[['dist',
                         'ang',
                         'wrist_shot',
                         'backhand',
                         'snap_shot',
                         'slap_shot',
                         'tip_in',
                         'deflected',
                         'wrap_around'
                         ]])
y = np.asarray(allShots['goal_binary'])

# Pre-scale data
X_pp = preprocessing.StandardScaler().fit(X).transform(X)

# Logistic Regression
X_train, X_test, y_train, y_test = train_test_split( X_pp, y, test_size=0.2, random_state=4)
X_train_dummy, X_test_dummy, y_train_dummy, y_test_dummy = train_test_split( X, y, test_size=0.2, random_state=4)
np.argwhere(np.isnan(X))
LR = LogisticRegression(C=0.01, solver='liblinear').fit(X_train, y_train)
yhat = LR.predict(X_test)
yhat_prob = LR.predict_proba(X_test)

t = yhat_prob[:,1]
res = pd.DataFrame(X_test_dummy)
res['preds'] = t

# Log - loss
ll_linear = log_loss(y_test, yhat_prob)
bs_preds = yhat_prob[:, 1]
bsc_linear = brier_score_loss(y_test, bs_preds)




# Best options - final testing
sample_allShots = allShots.sample(n=400000)
X = np.asarray(sample_allShots[['dist',
                         'ang',
                         'adjx',
                         'adjy',
                         'wrist_shot',
                         'backhand',
                         'snap_shot',
                         'slap_shot',
                         'tip_in',
                         'deflected',
                         'wrap_around'
                         ]])
y = np.asarray(sample_allShots['goal_binary'])
scaled_factors = preprocessing.StandardScaler().fit(X)
X_pp = scaled_factors.transform(X)

X_train, X_test, y_train, y_test = train_test_split(X_pp, y, random_state=7)
X_train_dummy, X_test_dummy, y_train_dummy, y_test_dummy = train_test_split( X, y, random_state=7)

lr = LogisticRegression( solver='liblinear').fit(X_train, y_train)
predictions_lr = lr.predict_proba(X_test)
score_lr = log_loss(y_test, predictions_lr)
print(score_lr)

gbc = GradientBoostingClassifier(n_estimators=100).fit(X_train, y_train)
predictions_gbc = gbc.predict_proba(X_test)
score_gbc = log_loss(y_test, predictions_gbc)
print(score_gbc)

knn400= KNeighborsClassifier(n_neighbors=400).fit(X_train, y_train)
predictions_knn400 = knn400.predict_proba(X_test)
score_knn400 = log_loss(y_test, predictions_knn400)
print(score_knn400)

knn1000= KNeighborsClassifier(n_neighbors=1000).fit(X_train, y_train)
predictions_knn1000 = knn1000.predict_proba(X_test)
score_knn1000 = log_loss(y_test, predictions_knn1000)
print(score_knn1000)



# Plot results for each option
types = []
types.append(('wrist_shot', 4))
types.append(('backhand', 5))
types.append(('snap_shot', 6))
types.append(('slap_shot', 7))
types.append(('tip_in', 8))
types.append(('deflected', 9))
types.append(('wrap_around', 10))
for shotType, num in types:
    # Testing
    # shotType = 'wrist_shot'
    # num = 4
    # Generate pointsdf with all zone points
    print(shotType)
    points = []
    for x in range(26, 89, 1):
        for y in range(-43, 43, 1):
            points.append((x, y))

    pointsdf = pd.DataFrame(data=points, columns=['x_coords', 'y_coords'])
    pointsdf['adjx'] = abs(pointsdf['x_coords'])
    pointsdf['adjy'] = abs(pointsdf['y_coords'])
    pointsdf['wrist_shot'] = 1 if shotType == 'wrist_shot' else 0
    pointsdf['backhand'] = 1 if shotType == 'backhand' else 0
    pointsdf['slap_shot'] = 1 if shotType == 'slap_shot' else 0
    pointsdf['snap_shot'] = 1 if shotType == 'snap_shot' else 0
    pointsdf['tip_in'] = 1 if shotType == 'tip_in' else 0
    pointsdf['deflected'] = 1 if shotType == 'deflected' else 0
    pointsdf['wrap_around'] = 1 if shotType == 'wrap_around' else 0
    # Distance and angle of location
    pointsdf['dist'] = np.sqrt(np.power((pointsdf['adjx'] - goal_x), 2) + np.power((pointsdf['y_coords'] - goal_y), 2))
    pointsdf['ang'] = 180 - (np.arctan2(pointsdf['adjy'], pointsdf['adjx']-100) * 180 / np.pi)

    # allZerodf = pd.DataFrame([89], columns=list('AB'))
    # allZerodf['adjx'] = 89
    # allZerodf['adjy'] = 0
    # allZerodf['wrist_shot'] = 0
    # allZerodf['wrist_shot'] = 0
    # allZerodf['backhand'] = 0
    # allZerodf['slap_shot'] = 0
    # allZerodf['snap_shot'] = 0
    # allZerodf['tip_in'] = 0
    # allZerodf['deflected'] = 0
    # allZerodf['wrap_around'] = 0
    # # Distance and angle of location
    # allZerodf['dist'] = np.sqrt(np.power((pointsdf['adjx'] - goal_x), 2) + np.power((pointsdf['y_coords'] - goal_y), 2))
    # allZerodf['ang'] = 180 - (np.arctan2(pointsdf['adjy'], pointsdf['adjx']-100) * 180 / np.pi)
    # pointsdf.append(allZerodf)

    zonePoints = np.asarray(pointsdf[['dist',
                             'ang',
                             'adjx',
                             'adjy',
                             'wrist_shot',
                             'backhand',
                             'snap_shot',
                             'slap_shot',
                             'tip_in',
                             'deflected',
                             'wrap_around',
                             'x_coords',
                             'y_coords'
                             ]])
    zonePoint_pp = np.asarray(pointsdf[['dist',
                                      'ang',
                                      'adjx',
                                      'adjy',
                                      'wrist_shot',
                                      'backhand',
                                      'snap_shot',
                                      'slap_shot',
                                      'tip_in',
                                      'deflected',
                                      'wrap_around'
                                      ]])
    # zonePoint_pp = preprocessing.StandardScaler().fit(zonePoint_pp).transform(zonePoint_pp)
    zonePoint_pp = scaled_factors.transform(zonePoint_pp)

    zonePreds_lr = lr.predict_proba(zonePoint_pp)
    zonePreds_gbc = gbc.predict_proba(zonePoint_pp)
    zonePreds_knn400 = knn400.predict_proba(zonePoint_pp)
    zonePreds_knn1000 = knn1000.predict_proba(zonePoint_pp)

    def plotType(num, yhat_prob, filt = 0, name=''):
        # TESTING
        # num = 4
        # yhat_prob = zonePreds_lr
        # filt = 25
        # name = 'lr'
        ####
        img = plt.imread("rinkoffensezone.png")

        t = yhat_prob[:, 1]
        res = pd.DataFrame(zonePoints)
        res['preds'] = t
        filtered = res[res[num] == 1]
        filtered = filtered[res[2] > filt]
        fig, ax = plt.subplots(1)
        plt.imshow(img, zorder=0, extent=[26, 89, -43, 43])
        p = ax.scatter(filtered[11], filtered[12], c=filtered['preds'], cmap='viridis', alpha=0.3)
        fig.colorbar(p)
        title = shotType + name
        plt.title(title)
        plt.show()

    plotType(num, zonePreds_lr, 25, 'lr')
    plotType(num, zonePreds_gbc, 25, 'gbc')
    plotType(num, zonePreds_knn400, 25, 'knn400')
    plotType(num, zonePreds_knn1000, 25, 'knn1000')


filename = 'finalized_model_knn_1000.sav'
pickle.dump(knn1000, open(filename, 'wb'))

filename = 'finalized_model_knn_1000_scaled_factors.sav'
pickle.dump(scaled_factors, open(filename, 'wb'))



# KNeighborsClassifier(n_neighbors=1000) ll = 0.279, AUC = 0.737

# Multiple test
# prepare models
models = []
models.append(('LR', LogisticRegression()))
models.append(('LDA', LinearDiscriminantAnalysis()))
models.append(('GBC', GradientBoostingClassifier(n_estimators=100)))
models.append(('KNN', KNeighborsClassifier(n_neighbors=100)))
models.append(('KNN', KNeighborsClassifier(n_neighbors=400)))
models.append(('CART', RandomForestClassifier(n_estimators=100)))
# models.append(('NB', GaussianNB()))
# models.append(('SVM', SVC()))
# evaluate each model in turn
results = []
names = []
scoring = 'accuracy'
seed = 7
for name, model in models:
    kfold = model_selection.KFold(n_splits=10, random_state=seed)
    # cv_results = model_selection.cross_val_score(model, X, y, cv=kfold, scoring=scoring)
    cv_results = model_selection.cross_val_predict(model, X_pp, y, cv=kfold, method='predict_proba')
    ll_result = log_loss(y, cv_results)
    auc = roc_auc_score(y, cv_results[:, 1])
    results.append(ll_result)
    names.append(name)
    msg = "%s: %f (%f)" % (name, ll_result, auc)
    print(msg)
# boxplot algorithm comparison
fig = plt.figure()
fig.suptitle('Algorithm Comparison')
ax = fig.add_subplot(111)
plt.boxplot(results)
ax.set_xticklabels(names)
plt.show()

test = models[3][1]
testingres = test.predict_proba(X_pp)



#
# sliced = allShots[:10000]
#
# mpl.rcParams['agg.path.chunksize'] = 100000
#
# x = sliced['adjx']
# y = sliced['y_coords']
# plt.plot(x, y, 'ro', alpha=0.1)
# plt.show()
#
# goals = allShots[allShots['event_type'] == 'Goal']
# print(goals.describe())
# x = goals['adjx']
# y = goals['y_coords']
# plt.plot(x, y, 'ro', alpha=0.01)
# plt.show()
#
# plt.scatter('x_coords', 'y_coords', c='event_desc', data=sliced)
# plt.xlabel('entry a')
# plt.ylabel('entry b')
# plt.show()









#
# ### Get connection to database and fetch previous plays
# hostname = 'localhost'
# username = 'kylelane'
# password = ''
# database = 'kylelane'
#
# myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
# cur = myConnection.cursor()
# test = cur.execute("""
#     SELECT * from  nhlstats.allplays
#     WHERE event_type = 'Shot' OR event_type = 'Goal' OR event_type ='Missed Shot'
#     """)
# rows = cur.fetchall()
#
# # commit sql
# myConnection.commit()
# # Close communication with the database
# cur.close()
# myConnection.close()