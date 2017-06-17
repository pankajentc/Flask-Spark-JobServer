#!/usr/bin/python

import json
import requests
from flask import Flask, request, stream_with_context, jsonify
app = Flask(__name__)

SPARK_DIR="/usr/local/spark/current"
SPARK_SUBMIT="/etc/init.d/spark-submit"

def exec_command(args):
    from subprocess import Popen, PIPE
    process = Popen(args, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        return (stdout+stderr,None)
    return (None,stdout)

def do_submit_job(jsonRequestParam):
    args=[SPARK_SUBMIT,"start",str(jsonRequestParam)]
    return exec_command(args)

@app.route("/spark-submit/start", methods=["POST"])
def submit_job():
    req = request.json
    error, out = do_submit_job(jsonRequestParam=req["jsonRequestParam"])
    f = open("logFile.txt", "wb")
    f.write(" With Configuration " +str(req["jsonRequestParam"]))
    f.flush()
    if error:
        return error, 400
    else:
        return out

def do_kill():
    args=[SPARK_SUBMIT,
        "stop"]
    error, out = exec_command(args)

    if error:
        return error, 400
    else:
        f = open("logFile.txt", "wb")
        f.write("")
        f.flush()
        return out

@app.route("/spark-submit/stop", methods=["DELETE"])
def kill():
    out = do_kill()
    return out

@app.route("/spark-submit/status", methods=["GET"])
def status():
    args=[SPARK_SUBMIT,
        "status"]
    error, out = exec_command(args)
    if error:
        return error, 400
    else:
        fo=open("logFile.txt", "r")
        return out + fo.read()


if __name__ == "__main__":
    app.run(host="0.0.0.0",port=8111,debug=True)