import math
import stemmer
import sqlite3

class BM:
	def __init__(self):
		self.porter = stemmer.Stemmer()
    
	def DB_build(self, post_list_path):
		self.param = {"idf_N":0, "icf_N":0,}
		
		self.conn = sqlite3.connect("tfidf.db")
		cur = self.conn.cursor()
		def file_to_dict(file_path):
			temp_post_list = {}
			col_N = 0
			doc_N = 0
			with open(file_path) as f:
				for line in f:
					w2d = line.replace("\n", "").strip().split(":")
					term, docs = w2d[0].strip().split(), w2d[1].strip().split()
					col_freq = int(term[1].replace("[", "").replace("]",""))
					col_N += col_freq
					temp_post_list[term[0]] = {"col_freq":col_freq, "docs":{}}
					for doc in docs:
						fnp = doc.split("#")
						f_path, term_freq = fnp[0], int(fnp[1])
						temp_post_list[term[0]]["docs"][f_path] = term_freq
					doc_freq = len(temp_post_list[term[0]]["docs"].items())
					temp_post_list[term[0]]["doc_freq"] = doc_freq
					doc_N += doc_freq
				return temp_post_list, col_N, doc_N
		self.posting_list, self.param["icf_N"], self.param["idf_N"] = file_to_dict(post_list_path)

		cur.execute("INSERT INTO META(idf, icf) VALUES(?, ?)", (self.param["idf_N"], self.param["icf_N"]))
   
		term_adder = "INSERT INTO TERMS(term, doc_freq, col_freq) VALUES(?, ?, ?)"
		doc_adder = "INSERT INTO DOCS(term, doc_id, freq) VALUES(?, ?, ?)"
    
		for term in self.posting_list:
			docs = self.posting_list[term]["docs"]
			doc_freq = self.posting_list[term]["doc_freq"]
			col_freq = self.posting_list[term]["col_freq"]
			cur.execute(term_adder, (term, doc_freq, col_freq))
			for doc_id in docs:
				cur.execute(doc_adder, (term, doc_id, docs[doc_id]))
		self.conn.commit();
		self.conn.close();
    
	def word_tf(self, term_freq):
		return 1+math.log(term_freq*1.0)
		
	def bm_word_tf(self, term_freq):
		return int(term_freq)
		
	def word_idf(self, idf, term_doc_freq):
		return math.log(idf*1.0/term_doc_freq)

	def doc_len_div_avg(self):
		with open("/home/cs13435/ir/report/doc/stemdAP88.txt") as ifp:
			doc_len = dict()  ##{DOCNO : int}
			doc_davg = dict() ##Ld / Lavg 
			doc = 0
			doc_count = 0
			                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
			for line in ifp:
				q = line.split()
				if len(q)==0:
					continue
			
				elif q[0]==('<DOCNO>'):
					if not doc_len:
						doc_count += 1						
						docno = q[1]
						continue
					else:
						doc = 0
						docno = q[1]
						doc_count += 1
				
				else:
					doc += len(q)
					if not doc_len:
						doc_len[docno] = 0
						doc_davg[docno] = 0
					else:
						doc_len[docno] = doc
						doc_davg[docno] = 0
						
			doc_all_len = 0
			doc_count = 0
			doc_len_avg = 0
			doc_id_len = 0
			for dn in doc_len:
				doc_count += 1
				doc_all_len += doc_len[dn]
				
			doc_len_avg = float(doc_all_len) / doc_count
			#ratio
			for dn in doc_len:			
				doc_davg[dn] = doc_len[dn] / float(doc_len_avg)
		return doc_davg
		
		
	def calc_sent_bm(self, sentence,doc_fLdLavg):
		
		doc_LdLavg = doc_fLdLavg
		
		self.conn = sqlite3.connect("/home/cs13422/tfidf_full.db")
		
		cur = self.conn.cursor()
		cur.execute("SELECT * FROM META")
		idf_N = cur.fetchall()[0][0]
		doc_finder = "SELECT * FROM DOCS WHERE term=?"
		term_finder = "SELECT * FROM TERMS WHERE term=?"  
       
		score_lst = {}
		query = sentence.strip().split()
		for term in query:
			query_term_freq = query.count(term)
			cur.execute(doc_finder, (term,))
			docs = cur.fetchall()
			if len(docs) == 0:
				continue
			cur.execute(term_finder, (term,))
			term_doc_freq = cur.fetchall()[0][1]
      			for doc in docs:
				doc_id = doc[1]
				term_freq = doc[2]
				
				ld_lavb = doc_LdLavg[doc_id]
				
				if doc_id in score_lst:					
					
					score_lst[doc_id] += self.word_idf(idf_N, term_doc_freq)*(3)*term_freq/(2*(0.25+(0.75*ld_lavb))+term_freq)*(2.5)*query_term_freq/(2+query_term_freq)
					
				else:					
					score_lst[doc_id] = self.word_idf(idf_N, term_doc_freq)*(3)*term_freq/(2*(0.25+(0.75*ld_lavb))+term_freq)*(2.5)*query_term_freq/(2+query_term_freq)
					
		self.conn.close();
		return score_lst

	def print_sorted_bm(self, sentence):
		sent = []
		for word in self.porter.remove_symbol(sentence.lower()).replace("\n","").split():
			sent.append(self.porter.stem(word, 0, len(word)-1))
		sent = " ".join(sent)
		sc_lst = self.calc_sent_bm(sent,doc_fLdLavg)
		sc_lst = sorted(sc_lst.items(), key=(lambda x:x[1]), reverse=True)
	
		print "stemmed input query: %s"%sent
		print " [doc_no | Okapi BM25_Score]"
		for doc, score in sc_lst[:5]:
			print " [%s | %f"%(doc, score)
		print "="*50
	def save_sorted_bm(self, save_path, query_file):
		save = open(save_path, 'w')
		start = 202
		querys = open(query_file)
		doc_fLdLavg = self.doc_len_div_avg()
		for query in querys:
			temp_query = []
			temp = self.porter.remove_symbol(query.lower()).replace("\n","").replace("\r","").split()
			for word in temp:
				temp_query.append(self.porter.stem(word, 0, len(word)-1))
			temp_query = " ".join(temp_query)

			sc_lst = self.calc_sent_bm(temp_query,doc_fLdLavg)
			sc_lst = sorted(sc_lst.items(), key=(lambda x:x[1]), reverse=True)
			#print("Query %d score calculation termination"%(start))
			for i, (doc, score) in enumerate(sc_lst[:1000]):
				save.write("%d Q%d %s %d %f %s\n"%(start, start-1, doc, i+1, score, "Namgung"))
			start += 1
		save.close()
		querys.close();

if __name__ == "__main__":
	scorer = BM()
#	scorer.DB_build("/home/cs13435/ir/report/doc/AP88_posting_list.txt")
#	print "DB_build!!"
 
	scorer.save_sorted_bm("kb_BM25_result.txt", "topics.202-250.txt")
	print "Complete!!"
	
