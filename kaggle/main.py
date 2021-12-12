def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100


if __name__ == '__main__':
    # %%
    import numpy as np
    import pandas as pd


    # %%
    def year_apply(year: int) -> int:
        if 0 <= year < 16:
            return year + 2000
        elif 52 <= year <= 99:
            return year + 1900
        else:
            return year


    # %%
    train_df = pd.read_csv('train.csv')
    train_df.fillna('undefined', inplace=True)
    train_df['registration_year'] = train_df['registration_year'].apply(lambda x: year_apply(x))

    test_df = pd.read_csv('train.csv')
    test_df.fillna('undefined', inplace=True)
    test_df['registration_year'] = test_df['registration_year'].apply(lambda x: year_apply(x))
    # %%
    from sklearn.model_selection import train_test_split

    X = train_df.drop('price', axis=1)
    y = train_df['price']

    categorical = X.select_dtypes(include=['object'])

    cars_dummies = pd.get_dummies(categorical, drop_first=True)
    cars_dummies.head()
    X = X.drop(columns=categorical)
    X = pd.concat([X, cars_dummies], axis=1)
    # %%
    X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                        train_size=0.7,
                                                        test_size=0.3, random_state=100)

    # %%

    # %%
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import LinearRegression, RidgeClassifier

    lm = RandomForestRegressor(n_estimators=50)
    lm.fit(X_train, y_train)

    y_pred_test = lm.predict(X_test)
    y_pred_train = lm.predict(X_train)
    # %%
    mean_absolute_percentage_error(y_test, y_pred_test)
