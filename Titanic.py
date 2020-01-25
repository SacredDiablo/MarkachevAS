#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split


# In[352]:


fl = pd.read_csv('train.csv')
test = pd.read_csv('test.csv')
passenger = pd.DataFrame({'PassengerID':test.PassengerId})


# In[353]:


fl.head()


# In[355]:


fl.isnull().sum()


# In[356]:


fl['Cabin'] = fl['Cabin'].fillna(0)
for i in range(len(fl.Cabin)):
    if fl.Cabin[i]!=0:
        fl.Cabin[i] = 1
test['Cabin'] = test['Cabin'].fillna(0)
for i in range(len(test.Cabin)):
    if test.Cabin[i]!=0:
        test.Cabin[i] = 1


# In[357]:


#при обучении PassengerId дает влияние на результат ~10%, но в реальности шанс выжить не зависить от этого
#Кроме этого очистим Embarked от NaN значений сразу
fl = fl.drop(['Name','Ticket','PassengerId'],axis=1)
fl = fl[pd.notnull(fl['Embarked'])]

test = test.drop(['Name','Ticket','PassengerId'],axis=1)
test = test[pd.notnull(test['Embarked'])]


# In[358]:


#Заменим все NaN значения в поле Age на средний возраст пассажиров в соответсии с классом купленных билетов
fl[fl.Pclass == 1] = fl[fl.Pclass == 1].fillna(fl[fl.Pclass == 1].Age.median())
fl[fl.Pclass == 2] = fl[fl.Pclass == 2].fillna(fl[fl.Pclass == 2].Age.median())
fl[fl.Pclass == 3] = fl[fl.Pclass == 3].fillna(fl[fl.Pclass == 3].Age.median())

test[test.Pclass == 1] = test[test.Pclass == 1].fillna(test[test.Pclass == 1].Age.median())
test[test.Pclass == 2] = test[test.Pclass == 2].fillna(test[test.Pclass == 2].Age.median())
test[test.Pclass == 3] = test[test.Pclass == 3].fillna(test[test.Pclass == 3].Age.median())


# In[359]:


#Для универсальности. Если остались NaN значения после всей обработки - удалим их
fl = fl.dropna(axis=0)

test = test.dropna(axis=0)


# In[360]:


#Поделим оставшиеся не числовыми значения на несколько столбцов с 0 и 1 (Sex, Embarked)
fl = pd.get_dummies(fl)

test = pd.get_dummies(test)


# In[361]:


#В результате получили неинформативный столбец Sex_male. Удалим его.
fl = fl.drop(['Sex_male'],axis=1)

test = test.drop(['Sex_male'],axis=1)


# In[362]:


fl.isnull().sum()


# In[363]:


test.isnull().sum()


# In[364]:


#Модель готова к обучению
X = fl.drop(['Survived'],axis=1)
y = fl.Survived


# In[365]:


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.3, random_state=42)


# In[366]:


clf = RandomForestClassifier(n_jobs=-1)


# In[367]:


#При большом количестве параметров обучение идет вечность, поэтому примем n_estimators по умолчанию за 200, max_depth = 21. Если качество модели не будет удовлетворять итеративно подберем все параметры.
parametrs = {'n_estimators':[200],'max_depth':[21],'min_samples_split':range(2,30,2), 'min_samples_leaf':range(2,30,2)}


# In[368]:


grid_search_forest = GridSearchCV(clf, parametrs,cv=5)


# In[369]:


get_ipython().run_cell_magic('time', '', 'grid_search_forest.fit(X_train, y_train)')


# In[370]:


grid_search_forest.best_estimator_


# In[371]:


grid_search_forest.best_estimator_.score(X_test, y_test)


# In[372]:


feature_importances = grid_search_forest.best_estimator_.feature_importances_
feature_importances_df = pd.DataFrame({'features':list(X_train),
                                       'feature_importances':feature_importances})
feature_importances_df.sort_values('feature_importances', ascending = False)


# In[373]:


survi = grid_search_forest.best_estimator_.predict(test)


# In[374]:


result = pd.DataFrame({'PassengerID':passenger.PassengerID,'Survived':survi})
result.Survived = result.Survived.astype(int)
filename = 'Titanic.csv'  
result.to_csv(filename,index=False)  
print('Saved file: ' + filename)


# In[ ]:




