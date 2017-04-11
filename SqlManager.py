#!/usr/bin/python
# -*- coding=utf-8 -*-
#python operate mysql database

import MySQLdb
import sys, getopt
import threading

#数据库名称
DATABASE_NAME = ''
#host = 'localhost' or '172.0.0.1'
HOST = 'localhost'
#用户名称
USER_NAME = 'root'
#数据库密码
PASSWORD = ''
#数据库编码
CHAR_SET = 'utf8'

class Singleton(object): 
    def __new__(cls, *args, **kw):  
        if not hasattr(cls, '_instance'):
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kw)  
        return cls._instance

class SqlManager(Singleton):	
	__db = None
	__mutex = None
	__databaseName = ''
	__host = ''
	__userName = ''
	__password = ''
	__charSet = ''
	
	def __init__(self):#,database_name,host,user_name,password,char_set):
		pass
		'''
		self.__databaseName = database_name
		self.__host = host
		self.__userName = user_name
		self.__password = password
		self.__charSet = char_set
		self.__mutex = threading.Lock()
		self.__db = MySQLdb.connect(host = host, user = user_name, passwd = password, db = database_name, charset = char_set)'''
	
	def __del__(self):
		self.closeConnect()
	
	def setConnect(self, database_name, host_name, user_name, password, char_set):
		if self.__db != None:
			print 'you have connected to mysql database!\nPlease disconnect and then setConnect!'
			return
		self.__databaseName = database_name
		self.__host = host_name
		self.__userName = user_name
		self.__password = password
		self.__charSet = char_set
		self.__mutex = threading.Lock()
		self.__db = MySQLdb.connect(host = host_name, user = user_name, passwd = password, db = database_name, charset = char_set)		
	
	def getCursor(self):#获取游标
		if self.__db != None:
			return __db.cursor()
		return None
	
	def closeConnect(self):#关闭连接
		if self.__db != None:
			self.__db.close()
			self.__db == None
	
	def closeCursor(self, cursor):#关闭cursor
		if cursor != None:
			cursor.close()
	
	def isConnect(self):
		if self.__db == None:
			print 'you haven\'t connected to mysql database!'
			return False
		else:
			return True
	
	def createTable(self,sql): #创建表
		print sql
		if (not self.isConnect()) or sql == '':			
			return False
		self.__mutex.acquire()
		cursor = self.__db.cursor()
		try:	
			cursor.execute(sql)
			self.__db.commit()
		except:
			print "Error: unable to create table"
			try:
				self.__db.rollback()
			except Exception, e:
				print "Error: rollback error", e
		cursor.close()
		self.__mutex.release()
		return True
		
	def queryTable(self, table_name = None, sql = None):
		print sql
		if (not self.isConnect()) or (table_name == None and sql == None):
			print "parameter error"
			return None
		result = None
		self.__mutex.acquire()
		cursor = self.__db.cursor()
		if sql == None and table_name != None:
			sql = "select *form %s"%table_name	
		try:
			cursor.execute(sql)
			result = cursor.fetchall()
			for row in result:
				print row
		except:
			print "Error: unable to fecth data"
		cursor.close()
		self.__mutex.release()
		return result
		
	def insertTable(self, sql, parameters = None):
		print sql,	self.isConnect()
		if (not self.isConnect()) or sql == '':				
			return False
		self.__mutex.acquire()
		cursor = self.__db.cursor()
		try:
			if parameters == None:
				cursor.execute(sql)				
			else:				
				cursor.execute(sql,parameters)
			self.__db.commit()# 提交到数据库执行
		except Exception, e:
			'''if sql.find('NAME'):
				parameters[2] = '格式错误'
				self.__mutex.release()
				self.insertTable(sql, parameters)
				self.__mutex.acquire()'''		
			print '发生错误时回滚', e
			try:
				self.__db.rollback()
			except Exception, e:
				if(e[0] == 2006):					
					cursor.close()
					self.closeConnect()
					self.__db = MySQLdb.connect(host = self.__host, user = self.__userName, passwd = self.__password, db = self.__databaseName, charset = self.__charSet)
					self.__mutex.release()					
					return self.insertTable(sql, parameters)
				print "Error: rollback error", e# 发生错误时回滚
		cursor.close()
		self.__mutex.release()
		return True
		
	def updateTable(self,sql, parameters = None):
		return self.insertTable(sql,parameters)		
	
	def deleteData(self,sql, parameters = None):
		return self.insertTable(sql,parameters)
	
	def showConnect(self):
		print '数据库连接信息:', __databaseName, __host, __userName, __charSet
		
	def showTables(self):
		if not self.isConnect():
			return None
		sql = 'show tables'
		result = None
		self.__mutex.acquire()
		cursor = self.__db.cursor()		
		try:
			cursor.execute(sql)			
			result = cursor.fetchall()
			print result
			for raw in result:
				print raw			
		except:
			print 'showTables error'
		cursor.close()
		self.__mutex.release()
		return result
		
	def showDatabases(self):
		if not self.isConnect():	
			return
		sql = 'show databases'
		cursor = self.__db.cursor()
		try:
			cursor.execute(sql)
			result = cursor.fetchall()
			for raw in result:
			 print raw
		except:
			print 'showDatabases error'
		cursor.close()
		
	def showTableStruct(self, table_name):
		if not self.isConnect():
			return
		sql = 'show cloumns from %s'%table_name
		self.__mutex.acquire()
		cursor = self.__db.cursor()
		try:
			cursor.execute(sql)
			result = cursor.fetchall()
			for raw in result:
			 print raw
		except:
			print 'showTableStruct error'
		cursor.close()
		self.__mutex.release()

def Usage():
	print 'SqlManage.py usage:'
	print '-h, --help: print help message'
	print '-d, --databaseName: input the database name (must)'
	print '-n, --hostName: input the host name ,(if None,default \"localhost\")'
	print '-u, --userName: input the user name ,(if None,default \"root\")'
	print '-p, --passwd: input the password ,(must)'
	print '-c, --charSet: input the char_set,(if None,default \"utf8\")'
	
def main(argv):	
	try:
		opts, args = getopt.getopt(argv[1:], 'hd:n:u:p:c:', ['databaseName=', 'hostName', 'userName=', 'passwd=', 'charSet='])
	except getopt.GetoptError, err:
		print str(err)
		Usage()
		sys.exit(2)
	for opt, value in opts:
		if opt in ('-h', '--help'):
			Usage()
			sys.exit(1)
		elif opt in ('-d', '--databaseName'):
			global DATABASE_NAME
			DATABASE_NAME = value 
		elif opt in ('-n', '--hostName'):
			global HOST
			HOST = value
		elif opt in ('-u', '--userName'):
			global USER_NAME
			USER_NAME = value
		elif opt in ('-p', '--passwd'):
			global PASSWORD
			PASSWORD = value
		elif opt in ('-c', '--charSet'):
			global CHAR_SET
			CHAR_SET = value
		else:
			print 'unhandled option'
			sys.exit(3)
	if DATABASE_NAME == '' or HOST == '' or PASSWORD == '' or CHAR_SET == '' or USER_NAME == '':
		print 'parameters error!'
		Usage()
		sys.exit(1)
			
	sqlManager = SqlManager()
	sqlManager.setConnect(DATABASE_NAME,HOST,USER_NAME,PASSWORD,CHAR_SET)
	sqlManager.showDatabases()
	sqlManager.showTables()
	print sqlManager.queryTable(sql="select *from %s where login_key = %s"%("TB_USER_LOGIN",1))

if __name__ == '__main__':
	main(sys.argv)	
		
