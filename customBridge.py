#!/usr/bin/python3
from flask import Flask, request,  make_response
from flask_restful import Resource, Api,  reqparse
from requests import get, put
import threading
import syslog
from time import sleep
import sys,  json
import socket
import traceback


app = Flask(__name__)
api = Api(app)
sem = threading.Semaphore()
def semAcquire(loc=""):
    sem.acquire()

def semRelease(loc=""):
    sem.release()

def arrivals(args):
    semAcquire("replan")
    planner.execPlan()
    planner.printGraph()
    semRelease("replan")
    
def startBackground():
    x = threading.Thread(target=arrivals, args=(1,))
    x.start()

class Order(Resource):
    # New OT
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('from',  type=str)
        parser.add_argument('to',  type=str)
        parser.add_argument('orderNumber',  type=str)
        parser.add_argument('admission',  type=bool)
        parser.add_argument('reservationId',  type=int)
        args = parser.parse_args()
        logger(syslog.LOG_NOTICE, request.remote_addr," New order request",  args)
        if args["from"] ==  args["to"]:
            logger(syslog.LOG_ERR," ", "New order fail, From and To are the same")
            return { "response": "FAIL", "msg": "From and To are the same" }, 400 # 400 (Bad Request)
        semAcquire("new ot")
        logger(syslog.LOG_DEBUG, "New order got semAquire " )
        try:
            ot = planner.newOT(args, request.remote_addr)
            planner.printGraph()
        except Exception as e:
            semRelease("new ot error") 
            print("New order fail ", e,traceback.format_exc() )
            logger(syslog.LOG_ERR, "New order fail ", e)
            #logger.exception(e)
            return { "response": "FAIL", "msg":str(e) }, 400 # 400 (Bad Request)
        semRelease("new ot")
        logger(syslog.LOG_DEBUG, "New order ok, replanning ",ot.id )
        replan()
        
        resp={ "response":"OK", "order_id": ot.id,  "from":ot.frm,  "to":ot.to,  "duration":ot.duration}
        logger(syslog.LOG_NOTICE, "New order  ",resp )
        return resp, 201 #201 (Created)

    # Order status
    def get(self, order_id):
        logger(syslog.LOG_NOTICE, request.remote_addr," Order status ",  order_id)
        ot=planner.findOT(order_id)
        if ot==None:
          return {}
        else:
            return { "order_id": ot.id,  "status":ot.status }

    def delete(self, order_id):
        logger(syslog.LOG_NOTICE, request.remote_addr," Order delete ",  order_id)
        if (planner.delOT(order_id)):
            return None, 200
        else:
            return "Order not found", 404

class Task(Resource):
    # Update Task: Fin de Tarea
    def put(self, task_id):
        parser = reqparse.RequestParser()
        parser.add_argument('action')
        parser.add_argument('photo',  required=False,  default="")
        parser.add_argument('desc',  required=False,  default="")
        
        args = parser.parse_args()
        logger(syslog.LOG_INFO,  request.remote_addr," Task update:",  task_id,  " action:",  args["action"])
        if args["action"] == "end":
            logger(syslog.LOG_NOTICE,  "End request :",  task_id)
            semAcquire("end task")
            msg=planner.endTask(task_id,  args)
            semRelease("end task")

            if msg == None:
                replan()
                msg="Done"
            logger(syslog.LOG_INFO,  "End request end:",  task_id)
            return {"task_id":task_id,"msg": msg}, 200 # 200 (OK)

        elif args["action"] == "status":
            logger(syslog.LOG_NOTICE,  "Status request:",  task_id)
            semAcquire()
            t=planner.findTask(task_id)
            if t == None:
                logger(syslog.LOG_ERR,  "Status request fail:",  task_id,  args)
                semRelease()
                return {"msg": "Task not found"},  404 # 404 (Not Found)
            else:
                logger(syslog.LOG_INFO,  "Status request end:",  task_id)
                st= t.getStatus()
                semRelease()
            return {"msg": st}  ,  200  # 200 (OK) 
            
        elif args["action"] == "fail":
            logger(syslog.LOG_NOTICE,  "Fail request:",  task_id,  " " , args)
            semAcquire()
            t=planner.findTask(task_id)
            if t == None:
                logger(syslog.LOG_WARNING,  " Fail request fail: task not found:",  task_id,  args)
                semRelease()
                return {"msg": "Task not found"},  404 # 404 (Not Found)
            elif t.getStatus() != "running":
                logger(syslog.LOG_WARNING,  " Fail request fail: task not running:",  task_id,  args)
                semRelease()
                return {"msg": "Task not running"},  404 # 404 (Not Found)
            else:
                logger(syslog.LOG_ERR,  "Fail  request Task Fail:",  task_id)
                # Marcamos la tarea en falla y el drop fuera de servicio
                t.setFail()
                planner.setOutOfService()
                planner.sendEventNotice(t.kart.type, t.kart.name, args["desc"] if 'desc' in args else 'N/A' )
                semRelease()
                replan()
            return {"task_id":task_id,"msg": "ok"}, 200 # 200 (OK)

        else:
            logger(syslog.LOG_ERR,  "Invalid Task Action:",  task_id)
            return {"msg": "no valid action"},  405 # 405 (Method Not Allowed)
                
class Kart(Resource):
    # Update Task: Checkin
    def put(self, kart_id):
        parser = reqparse.RequestParser()
        parser.add_argument('ip')
        parser.add_argument('port',  type=int)
        args = parser.parse_args()
        k = getKart(kart_id)
        if k == None:
            logger(syslog.LOG_ERR,  request.remote_addr," Checkin planner:",   kart_id ,  " not found")
            return { "response":"FAIL", "msg":  kart_id + " not found"} ,  404 # 404 (Not Found)
        else:
            logger(syslog.LOG_NOTICE,  request.remote_addr," Checkin ", kart_id, " standaloneMode:",  planner.standaloneMode,  " args:",  args)
            semAcquire()
            k.connect(args['ip'], args['port'])
            isLastKart= isAllKartsReady()
            semRelease()
            replan()
            
            if (isLastKart):
                if ( not planner.standaloneMode):
                    while True:
                        logger(syslog.LOG_INFO, "sending confirmation to Manager")
                        url=hostDropManager+"/register"
                        try:
                            put(url, timeout=5)                        
                            logger(syslog.LOG_DEBUG, "Register OK on manage")
                            break
                        except:
                            #TODO: el ultimo kart checkin queda bloqueado hasta que se connecte con manager 
                            logger(syslog.LOG_WARNING,  "Fail register in Manager")
                            sleep(5)
                logger(syslog.LOG_NOTICE,  "Checkin end, Last kart!")
            else:            
                logger(syslog.LOG_NOTICE,  "Checkin end")
            return {"response":  "OK", "kart_id":str(kart_id) ,  "ip":str(k.ip),  "port":str(k.port),  "config":{"line_len":k.length} } ,  200  # 200 (OK)   
    # Traspasa un evento al drop manager, para telemetria
    def post(self, code, component):
        logger(syslog.LOG_NOTICE,  request.remote_addr," Telemetria ",  component,  ":" ,code)
        component = component.upper()
        code = code.upper()
        
        if (component == 'KART'):
            sendEventFromKart(code)
        elif (component == 'LIFT'):
            sendEventFromLift(code)
        else:
            logger(syslog.LOG_ERR,'Telemetria, componente enviado no es valido', component)
            return { 'response': 'BAD_REQUEST', 'msg': 'component not valid' }, 400 # 400 (BAD_REQUEST)
        
        logger(syslog.LOG_INFO,"Telemetria",  "all done")
        return { 'response': 'OK', 'msg': 'sending event success' }, 200 # 200 (OK) 

    # Kart status
    def get(self, kart_id):
        k = getKart(kart_id)
        if k == None:
            logger(syslog.LOG_ERR,  "Get kart:",   kart_id ,  " not found")
            return { "response":"FAIL", "msg":  kart_id + " not found"} ,  404 # 404 (Not Found)
        else:
            logger(syslog.LOG_NOTICE,  "get ", kart_id)
            return { 'response': 'OK', 'name':   k.name,  'type' :  k.type, 'length': k.length,  'height' : k.height,  'color' : k.color,  'ip' : k.ip ,  'port':k.port,  'fail': k.failure }, 200 # 200 (OK) 
 
class PlannerServer (Resource):
    def get(self,  action, kart_id=None):
        logger(syslog.LOG_NOTICE,  request.remote_addr," Planner status",  action)
        print( "Planner ",  action)
        
        # Over all status
        if (action=="status"):
            return {"response":  "OK", 
            "status":str("Im Happy") ,  
            "response": "OK",
            "task_id": 0,
            "task_type": "status",
            "name": socket.gethostname(),
            "type": "planner", 
            "karts":getKartList(),  
            "connected": getConnectedKarts(),  
            "layout":planner.getLayout() }  ,  200  # 200 (OK)         
          
        # Current plan status in .dot format
        elif (action == "plan"):
            semAcquire()
            response = make_response(str(planner.getDotPlan()),  200)
            semRelease()
            response.mimetype = "text/plain"
            return response
        elif (action == "positions"):
            k = getKart(kart_id)
            if k == None:
                logger(syslog.LOG_ERR,  request.remote_addr," kart positions:",   str(kart_id) ,  " not found")
                return { "response":"FAIL", "msg":  str(kart_id) + " not found"} ,  404 # 404 (Not Found)
            else:
                return k.positions
        elif (action == "transfers"):
            k = getKart(kart_id)
            if k == None:
                logger(syslog.LOG_ERR,  request.remote_addr," kart transfers:",   str(kart_id) ,  " not found")
                return { "response":"FAIL", "msg":  str(kart_id) + " not found"} ,  404 # 404 (Not Found)
            else:
                return planner.kartTransfers[kart_id]
        elif (action == "outOfService"):
            planner.setOutOfService(True)
            return { "response":"OK", "msg":  action} ,  200 # 200 OK
        elif (action == "onService"):
            planner.setOutOfService(False)
            return { "response":"OK", "msg":  action} ,  200 # 200 OK
        else:
            return None ,  405 # 405 (Method Not Allowed)
            
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('task_type')
        args = parser.parse_args()
        logger(syslog.LOG_INFO,  request.remote_addr," Request:",  args)
        
        if args["task_type"] == "status":
            return {
            "response": "OK",
            "tdropask_id": 0,
            "task_type": "status",
            "name": socket.gethostname(),
            "type": "planner"
            }, 200
        else:
            return None, 405
            
api.add_resource(PlannerServer, '/<string:action>','/<string:action>/<string:kart_id>')          # Kart list status, and plan
api.add_resource(Order, '/order', '/order/<int:order_id>')                        # POST New order, GET Order status ,DELETE Order    ,  '/order/<int:order_id>',  endpoint="order_id"
api.add_resource(Task, '/task/<int:task_id>')  # End task
api.add_resource(Kart, '/kart/<string:kart_id>', '/kart/<string:component>/<string:code>')   # Checkin and send event

if __name__ == '__main__': 
    planner.standaloneMode=False
    logger(syslog.LOG_NOTICE,  "Planner starting")
    try:
        if len(sys.argv)  == 1:
            url=hostDropManager+"/setup"
            print("Conectamos al drop manager ", url)
            logger(syslog.LOG_NOTICE,  "Conectamos al drop manager ", url)
            retry=True;
            content=""
            while retry:
                try:
                    content = get(url,  timeout=5).json();
                    retry=False
                    print("Connection success with Drop Manager")
                except:  # This is the correct syntax
                    # except  ConnectionRefusedError
                    print("Connection fail with Drop Manager, retry...")
                    logger(syslog.LOG_WARNING,  "Connection fail with Drop Manager, retry...", url)
                    sleep(5)
                    retry=True
        elif len(sys.argv)  == 2:
            planner.standaloneMode=True
            logger(syslog.LOG_NOTICE,  "STANDALONE MODE, Usamos archivo de configuracion ",  sys.argv[1])
            with open(sys.argv[1], 'r') as f:
                content = json.load(f)
        else:
            logger(syslog.LOG_ERR,  "Wrong api syntax",  sys.argv)
            raise Exception("Syntax: api.py [conf_file]")
        planner.initPlannerJson("cmd.log", content)
        resetKarts()
        app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024
        app.run(debug=False, host= '0.0.0.0')
    except KeyboardInterrupt:
        pass
    finally:
        pass
        #stopAllKarts()
