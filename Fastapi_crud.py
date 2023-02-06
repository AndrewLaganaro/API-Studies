from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from fastapi import FastAPI
from typing import List, Optional, Dict 
from os import environ
import json

Base = declarative_base()
app = FastAPI()

class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key = True)
    title = Column(String)
    description = Column(String)

def connect_to_database(database_type):
    db_url = environ.get(f"{database_type.upper()}_URL")
    if not db_url:
        raise ValueError(f"{database_type.upper()}_URL not set in environment variables")

    match database_type:
        case "sqlite":
            engine = create_engine(db_url)
        case "postgres":
            db_user = environ.get("POSTGRES_USER")
            db_password = environ.get("POSTGRES_PASSWORD")
            engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_url}")
        case "mysql":
            db_user = environ.get("MYSQL_USER")
            db_password = environ.get("MYSQL_PASSWORD")
            engine = create_engine(f"mysql://{db_user}:{db_password}@{db_url}")
        case _:
            raise ValueError(f"Unsupported database type: {database_type}")

    return engine

def build_response(status, content, perf, details, multiple, error = None, perf_idx = None, obtained = None):
    response = {}
    messages = {
                "Status":  ["Failed", "Success"],
                "Content": ["Error", "Warning", "Message"],
                "Error":   ["No found", "not found", "already exists",
                            "already existed", "doesn't exists"],
                "Performed": {
                    "Get": ["Group", "Found"],
                    "Post": "created",
                    "Put": "updated",
                    "Del": "deleted"},
                "Details": ["Called", "Existing"],
                "Multiple":["Task","Tasks","All"]
                }
    # build_response(1, -1, -1, "Get", 0, -1, obtained = task_list, act_idx = 0)
    
    rstatus = messages["Status"][status]
    tcontent = messages["Content"][content]
    if content == "Error" or "Warning":
        if error == 0:
            mcontent = messages["Error"][error][:3] + messages["Multiple"][multiple] + messages["Error"][error][3:]
        else:
            mcontent = messages["Multiple"][multiple] + ' ' + messages["Error"][error].lower()
        
    elif content == "Message":
        if perf != -1:
            if perf_idx == 1:
                mcontent = messages["Multiple"][multiple]  + ' ' + messages["Performed"][perf][perf_idx].lower() # "task" + ' ' + "found"
        else:
            mcontent = messages["Multiple"][multiple] + ' ' + messages["Performed"][perf]
            
    if perf != -1:
        
        if perf_idx == 0:
            rperf = messages["Performed"][perf][perf_idx] + ' ' + messages["Multiple"][multiple].lower() # "Group" + ' ' + "task"
        if perf_idx == 1:
            rperf = messages["Multiple"][multiple] + ' ' + messages["Performed"][perf][perf_idx].lower() # "task" + ' ' + "found"
        if perf_idx == 2:
            rperf = messages["Multiple"][perf_idx] + ' ' + messages["Multiple"][multiple].lower() # All + ' ' + "tasks"
    else:
        rperf = messages["Multiple"][multiple] + ' ' + messages["Performed"][perf] # "tasks" + ' ' + "created"
        
    if details != -1:
        rdetails = messages["Details"][details] + ' ' + messages["Multiple"][multiple]
    
    response.update({"Status":rstatus})
    response.update({tcontent: mcontent})
    args = {"details": details, "perf": perf}
    for arg in args:
        if arg["perf"] != -1:
            response.update({rperf: obtained})
        if arg["details"] != -1:
            response.update({rdetails: obtained})
    
    
    {"Status": "Success",
    "Message": "Tasks found",
    "All tasks": task_list}
    build_response(1, -1, -1, "Get", 0, -1, obtained = task_list, perf_idx = 0)
    
    {"Status": "Success",
    {"Status": "Failed",
    "Error": "No tasks found"}
    
    {"Status": "Success",
    "Message": "Task created",
    "Task": {"id": task.id, "title": task.title, "description": task.description}}
    
    {"Status": "Failed",
    "Error": "Task not found"}
    
    {"Status": "Success",
    "Message": "Task created",
    "Task created": new_task}
    
    {"Status": "Failed",
    "Error": "The task already exists",
    "Existing task": task}
    
    {"Status": "Success",
    "Warning": "Some tasks already existed",
    "Tasks created" : new_tasks, 
    "Existing tasks": old_tasks}
    
    {"Status": "Failed",
    "Error": "All tasks already existed",
    "Existing tasks": old_tasks}
    {"Status": "Success",
    "Message": "All tasks created",
    "Tasks created" : new_tasks}
    {"Status": "Failed",
    "Error": "The task doesn't exists",
    "Called task": (task_id, task)}
    {"Status": "Success",
    "Message": "Task Updated",
    "Updated task": task}
    {"Status": "Success",
    "Message": "Task Deleted",
    "Deleted task": task_id}
    {"Status": "Failed",
    "Error": "The task doesn't exists",
    "Called task": task_id}
    {"Status": "Success",
    "Message": "Task Deleted",
    "Deleted task": task_id}
    {"Status": "Failed",
    "Error": "The tasks doesn't exists",
    "Called tasks": abst_tasks}
    {"Status": "Success",
    "Message": "Tasks Deleted",
    "Deleted tasks": task_ids}

    return response

@app.get("/task")
def get_all_tasks(internal: bool = False):
    session = Session()
    tasks = session.query(Task).all()
    session.close()
    if tasks:
        task_list = []
        for task in tasks:
            task_list.append({"id": task.id, "title": task.title, "description": task.description})
        response = {"Status": "Success",
                    "Message": "Tasks found",
                    "All tasks": task_list}
    else:
        response = {"Status": "Failed",
                    "Error": "No tasks found"}
    if internal:
        return task_list
    return response

@app.get("/task/group")
def get_tasks(task_ids: List[int]):
    session = Session()
    tasks = session.query(Task).filter(Task.id.in_(task_ids)).all()
    session.close()
    if task:
        task_list = []
        for task in tasks:
            task_list.append({"id": task.id, "title": task.title, "description": task.description})
        response = {"Status": "Success",
                    "Message": "Tasks found",
                    "Group tasks": task_list}
    else:
        response = {"Status": "Failed",
                    "Error": "Tasks not found"}
    return response

@app.get("/task/{task_id}")
def get_one_task(task_id: int):
    session = Session()
    task = session.query(Task).get(task_id)
    session.close()
    if task:
        response = {"Status": "Success",
                    "Message": "Task found",
                    "Task found": {"id": task.id, "title": task.title, "description": task.description}
                    }
    else:
        response = {"Status": "Failed",
                    "Error": "Task not found"}
    return response


@app.post("/task")
def create_one_task(task: Task):
    session = Session()
    new_task = Task(title = task.title, description = task.description)
    if not task_exists(None, new_task):
        session.add(new_task)
        session.commit()
        session.close()
        response = {"Status": "Success",
                    "Message": "Task created",
                    "Task created": new_task}
    else:
        response =  {"Status": "Failed",
                    "Error": "The task already exists",
                    "Existing task": task}
    return response

@app.post("/tasks/group")
def create_tasks(tasks: List[Task]):
    session = Session()
    old_tasks, new_tasks, response = [], [], {}
    for task in tasks:
        new_task = Task(title=task.title, description=task.description)
        if not task_exists(None, new_task):
            new_tasks.append(new_task)
        else:
            old_tasks.append(new_task)
            response = {"Status": "Success",
                        "Warning": "Some tasks already existed",
                        "Tasks created" : new_tasks, 
                        "Existing tasks": old_tasks}
    if new_tasks == old_tasks:
        response = {"Status": "Failed",
                    "Error": "All tasks already existed",
                    "Existing tasks": old_tasks}
        
    session.add_all(new_tasks)
    session.commit()
    session.close()
    response = {"Status": "Success",
                "Message": "All tasks created",
                "Tasks created" : new_tasks}
    return response

def task_exists(task_id = None, task = None):
    session = Session()
    if task_id:
        got_task = session.query(Task).get(task_id)
    else:
        task_list = get_all_tasks(internal=True)
        for got_task in task_list:
            if task.title and task.description == got_task.title and got_task.description:
                return True
    session.close()
    return True if got_task else False

@app.put("/task/{task_id}")
def update_task(task_id: int, task: Task):
    session = Session()
    if not task_exists(task_id):
        response = {"Status": "Failed",
                    "Error": "The task doesn't exists",
                    "Called task": (task_id, task)}
        session.close()
        return response
    task_to_update = session.query(Task).get(task_id)
    task_to_update.title = task.title
    task_to_update.description = task.description
    session.commit()
    session.close()
    response =  {"Status": "Success",
                "Message": "Task Updated",
                "Updated task": task}
    return response

@app.delete("/task/{task_id}")
def delete_task(task_id: int):
    session = Session()
    if not task_exists(task_id):
        response = {"Status": "Failed",
                    "Error": "The task doesn't exists",
                    "Called task": task_id}
        session.close()
        return response
    task_to_delete = session.query(Task).get(task_id)
    session.delete(task_to_delete)
    session.commit()
    session.close()
    response =  {"Status": "Success",
                "Message": "Task Deleted",
                "Deleted task": task_id}
    return response

@app.delete("/task/{task_ids}")
def delete_tasks(task_ids: List[int]):
    session = Session()
    abst_tasks, del_tasks = [], []
    for task_id in task_ids:
        if task_exists(task_id):
            del_tasks.append(task_id)
        if not task_exists(task_id):
            abst_tasks.append(task_id)
            response = {"Status": "Failed",
                        "Error": "The tasks doesn't exists",
                        "Called tasks": abst_tasks}
    tasks_to_delete = session.query(Task).filter(Task.id.in_(task_ids)).all()
    session.delete(tasks_to_delete)
    session.commit()
    session.close()
    response =  {"Status": "Success",
                "Message": "Tasks Deleted",
                "Deleted tasks": task_ids}
    return response

engine = connect_to_database("postgres")
Base.metadata.create_all(engine)
Session = sessionmaker(bind = engine)

