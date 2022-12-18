import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import sqlite3

from preprocess import tokenize

DB_NAME = "descriptions1.db"


class Vacancies:
    def __init__(self, vectorizer_type='tfidf', n_top=10):
        self.vectorizer_type = 'tfidf'
        self.descriptions_df = pd.read_sql_query("SELECT * FROM hh_descriptions", con)
        self.tokens_df = pd.read_sql_query("SELECT * FROM hh_tokens", con)
        #countvectorizer = CountVectorizer(analyzer='word',stop_words='english')
        #count_wm = countvectorizer.fit_transform(df.description)
        #count_tokens = countvectorizer.get_feature_names_out()
        #df_countvect = pd.DataFrame(data=count_wm.toarray(), columns=count_tokens)
        self.tfidfvectorizer = None
        self.tfidf_wm = None
        self.tfidf_tokens = None
        self.df_tfidfvect = None
        self.n_top = n_top

    def fit(self):
        self.tfidfvectorizer = TfidfVectorizer(analyzer='word', stop_words='english')
        self.tfidf_wm = self.tfidfvectorizer.fit_transform(self.descriptions_df.description)
        self.tfidf_tokens = self.tfidfvectorizer.get_feature_names_out()
        self.df_tfidfvect = pd.DataFrame(data=self.tfidf_wm.toarray(), columns=self.tfidf_tokens)

    def _find_top_closest_ids(self, resume_text, ntop):
        resume_df = pd.DataFrame(self.tfidfvectorizer.transform([tokenize(resume_text)]).toarray(), columns=self.tfidf_tokens)
        df2 = pd.DataFrame(cosine_similarity(self.tfidf_wm, resume_df, dense_output=True), columns=['score'])
        closest_ids = df2.sort_values('score', ascending=False).iloc[:ntop]
        return closest_ids

    def _get_vacs_from_ids(self, ids):
        return ids.join(self.descriptions_df, how='inner')[["title", "description", "url", "score"]]\
            .to_json(orient="records", force_ascii=False)
    
    def get_matching_vacancies(self, resume_text):
        ids = self._find_top_closest_ids(resume_text, self.n_top)
        res = self._get_vacs_from_ids(ids)
        return res



if __name__ == '__main__':
    try:
        resume_text = 'Data Scientist с опытом работы в проектах по продвинутой аналитике и исследованиях. Успешно реализовал проекты включая промышленные решения \
            для клиентов из различных областей: потребительские товары, ритейл, медицинские приборы и другие. Имеет страсть к автоматизации \
            на основе данных. Европейское гражданство. Разработка, алгоритмы, машинное обучение, проведение интервью\
            Data Science, Leadership, R&D, Machine Learning, Software Developement, Algorithms, Agile, Interviewing \
            Python (numpy, pandas, scipy, sklearn, matplotlib, seaborn, requests), DevOps (Linux, Git, Docker), Cloud (MS Azure, AWS),\
            WEB (Flask, Figma, HTML, CSS), Business Intelligence (Power BI), SQL'

        con = sqlite3.connect(DB_NAME)
        vac_model = Vacancies()
        vac_model.fit()
        print(vac_model.get_matching_vacancies(resume_text))
    except Exception as e:
        print(e)
    finally:
        con.close()


