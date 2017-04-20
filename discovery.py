'''
preprocess all the emails
Created on Nov 26, 2016

@author: Zhongchao
'''

import email
import re
import json
from os import listdir, chdir
from nltk.corpus import stopwords
import nltk
import csv
import sys

'''
global parameters
'''
attach_pat = re.compile(r'- [0-9a-zA-Z]+\.(doc|DOC|xls|XLS|txt|TXT|jpg|JPG|png|PNG)')
extract_address = re.compile(r'^\s*([^@])@(.*)');
ps = nltk.stem.PorterStemmer()

all_send = {}
all_receive = {}
all_words = []
all_emails = 0
total_length = 0
all_important_words = []
emailaddress = ""
fullname = ""
important_words = []
all_s = {}
all_r = {}
all_data = []

path = ""
savepath = ""
output_file = ""


'''
strip name
'''
def stripName(name, target):
    if name.find(".") > 0:
        fname = name[:name.find(".")]
        lname = name[name.find(".")+1:]
    else:
        return False
    if target.find("-") > 0:
        ltarget = target[:target.find("-")]
        ftarget = target[target.find("-")+1:]
    if lname and ltarget and fname and ftarget and lname == ltarget and fname[:1] == ftarget:
        return True
    else:
        return False   
        
'''
extract Header information from email
'''
def extractHeader(msg, name, file):
    global fullname
    global emailaddress
    global all_important_words
    global important_words
    global r_s
    c_r = []
    c_s = []
    flag = False
    receiver_string = msg['To']
    sender = msg['From']
    subject = msg['Subject']
    
    if receiver_string and sender: # could also ask "isinstance(sender,str)"
            
        receiver_string.strip()
        receiver_list = [x.strip() for x in receiver_string.split(",")]
        receiver_set = set(receiver_list)
        
        tmp = sender
        sender.strip()
        sender = sender[:sender.find("@")]
        
        if stripName(sender, name):
            flag = True
            tmp = tmp.strip()
            emailaddress = tmp
            fullname = sender
        if flag == False:
            for receiver in receiver_set:
                tmp = receiver
                receiver = receiver[:receiver.find("@")]
                if stripName(receiver, name):
                    flag = True
                    tmp = tmp.strip()
                    emailaddress = tmp
                    fullname = receiver
                    break
                else:
                    continue
        
        if flag:
            subject = subject.lower()
            subject = remove_stopwords(subject)
            subject = stemmer(subject)
            all_important_words += subject
            important_words += subject
            
            if stripName(sender, name):
                for receiver in receiver_set:                    
                    receiver = receiver[:receiver.find("@")]
                        
                    if not all_send.has_key(receiver):
                        all_send[receiver] = 0 # initialize
                    all_send[receiver] += 1
                    c_r.append(receiver)
                all_r[file[file.rfind('/', 0, len(file))+1:]] = c_r
            else:
                for receiver in receiver_set:                    
                    receiver = receiver[:receiver.find("@")]
                    
                    if stripName(receiver, name):    
                        if not all_receive.has_key(sender):
                            all_receive[sender] = 0 # initialize
                        all_receive[sender] += 1
                c_s.append(sender)                           
                all_s[file[file.rfind('/', 0, len(file))+1:]] = c_s
             
    return flag

'''
extract Body information from email
'''
def extractBody(msg):
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))
    
            # skip any text/plain (txt) attachments
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body = part.get_payload(decode=True)  # decode
                break
    # not multipart - i.e. plain text, no attachments, keeping fingers crossed
    else:
        body = msg.get_payload(decode=True)
    return body
    
'''
clean Header information from email Body
'''
def cleanHeader(body):
    cleaned = ""
    while body.find("To:") > -1:
        ini1 = body.find("Original Message")
        end1 = body.find("Subject:", ini1)
        end1 = body.find("\n", end1)
        ini2 = body.find("- Forwarded by")
        end2 = body.find("Subject:", ini2)
        end2 = body.find("\n", end2)
        ini3 = body.find("To:")
        ini3 = body.rfind("\n", 0, ini3-2)
        end3 = body.find("Subject:", ini3)
        end3 = body.find("\n", end3)
        if ini1 > -1:
            body = body[:ini1-1] + body[end1:]
        elif ini2 > -1:
            body = body[:ini2-1] + body[end2:]  
        elif ini3 > -1:
            body = body[:ini3-1] + body[end3:]  
        elif ini3 == -1 and body.find("To:") > -1:
            body = body[:body.find("To:")]
        else:
            body = body[body.find("\n",body.find("To:"))+1:]
    cleaned += body
    return cleaned

'''
clean attachment information from email Body
'''
def cleanAttach(body):
    while re.search(attach_pat, body):
        body = attach_pat.sub('', body)
    return body

'''
tokenize body and remove stop words
'''
def remove_stopwords(sentence):
    words = re.findall(r'\w+', sentence, flags = re.UNICODE | re.LOCALE)
    no_stopwords=[]
    for word in words:
        if word not in stopwords.words('english'):
            no_stopwords.append(word)
    return no_stopwords

'''
stem words
'''
def stemmer(words):
    stemmer_words = []
    for word in words:
        stemmer_words.append(str(ps.stem(word)))
    return stemmer_words

'''
save results
'''
def saveResults():
    print "\nall receivers:"
    for receiver in all_send.keys(): # cycle over for output
        print receiver, all_send[receiver]
    print "\nall senders:"
    for sender in all_receive.keys(): # cycle over for output
        print sender, all_receive[sender]
    all_contacters = all_send.copy()
    all_contacters.update(all_receive)
        
    json.dump(all_send, open(output_file+"all_send",'w'))
    json.dump(all_receive, open(output_file+"all_receive",'w'))
    
    summary = {}
    summary["name"] = fullname
    summary["email address"] = emailaddress
    summary["number of emails"] = all_emails
    if all_emails == 0:
        summary["average length"] = 0
    else:
        summary["average length"] = total_length / all_emails
    summary["all contacters"] = len(all_contacters)
    summary["all senders"] = all_receive
    summary["all receivers"] = all_send
    json.dump(summary, open(output_file+"summary",'w'))
    
    json.dump(all_s,  open(output_file+"all_s", 'w'))
    json.dump(all_r,  open(output_file+"all_r", 'w'))
    
    with open(output_file+"processedemails.csv","w") as f:
        wr = csv.writer(f)
        wr.writerows(all_data)

'''
save Text
'''
def saveText(important_words, filename):
    with open(filename, "w") as f:
        for s in important_words:
            #f.write(str(s) +"\n")
            f.write(str(s) +" ")
 

'''
mainFunc called by the main.py
'''
def mainFunc(user): 
    global path, savepath, output_file, all_send, all_receive, all_words, all_emails, total_length
    global all_important_words, emailaddress, fullname, important_words, all_s, all_r, all_data
    path = "D:/course/bigdata/Assign/ass3/maildir/" + user + "/all_documents"
    savepath = "D:/course/bigdata/Assign/ass3/maildir/" + user + "/output/test"
    output_file = "D:/course/bigdata/Assign/ass3/maildir/" + user + "/output/"

    all_send = {}
    all_receive = {}
    all_words = []
    all_emails = 0
    total_length = 0
    all_important_words = []
    emailaddress = ""
    fullname = ""
    important_words = []
    all_s = {}
    all_r = {}
    all_data = []     
    
          
    chdir(path)
    files = listdir('.')
    files = [path + "/" + f for f in files]
    for f in files:
        important_words = []
        msg = email.message_from_file(open(f))  
        curflag = extractHeader(msg, user, f)
        if curflag:
            print f
            body = extractBody(msg)       
            cleaned = cleanHeader(body)
            cleaned = cleanAttach(cleaned)
            total_length += len(cleaned)
            all_emails += 1
            cleaned = cleaned.lower()
            cleaned = remove_stopwords(cleaned)
            cleaned = stemmer(cleaned)
            all_important_words += cleaned
            important_words += cleaned
            filename = f[f.rfind('/', 0, len(f))+1:]
            element_tuple = str_tuple = [filename, " ".join(important_words)]  
            all_data.append(element_tuple)      
            #saveText(important_words, filename)
    
    saveResults()

