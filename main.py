import random
from google.cloud import storage
from flask import Flask, request, make_response, Response
from google.cloud.storage import Blob

app = Flask(__name__)

storage_client = storage.Client()

def getBucket(id):
    buckets = storage_client.list_buckets()
    for i in buckets:
        try:
            if i.labels['id'] == id:
              return i
        except:
            a=1
    return False


def deleteBucket(id):
    bucket = getBucket(id)
    if bucket == False:
        resp = make_response({})
        resp.status_code = 404
    else:
        bucket.delete()
        resp = make_response(f"Bucket with id {id} has been deleted!")
        resp.status_code = 200
    return resp


def postBucket(name, storageClass, bucket_id):
    bucket = storage_client.bucket(name)
    bucket.storage_class = storageClass
    bucket.labels = {"id": str(bucket_id)}
    new_bucket = storage_client.create_bucket(bucket, location="europe-west3")
    return new_bucket


def getBucketInfo(bucket):
    if bucket is []:
        buckets_dict = {}
    else:
        bucket_dict = {"name": bucket.name, "storageClass": bucket.storage_class}
    return bucket_dict


@app.errorhandler(404)
def not_found(item, url):
    return {"Item": item, "Status": "Resource not found",
            "Actions": {"CreateResourceById": {"URL": url, "METHOD": "PUT"},
                        "CreateResourceBlindly": {"URL": url.split("/")[0], "METHOD": "POST"}}}


@app.errorhandler(405)
def method_not_allowed(method, url, allowed_methods):
    return {"URL": url, "UnallowedMethod": method, "AllowedMethods": allowed_methods}


@app.errorhandler(400)
def bad_request(method, url):
    return {"URL": url, "Method": method, "Status": "Bad request"}


@app.route('/buckets', methods=["GET", "POST"])
def buckets():
    if request.method not in ["GET", "POST"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    resp = Response("")

    if request.method == "GET":
        # descarca toate `galetile` + obiectele din ele? not ideal

        buckets = storage_client.list_buckets()
        buckets = list(filter(lambda x: "id" in x.labels.keys(), buckets))

        if buckets is []:
            buckets_dicts = {}
        else:
            buckets_dicts = {
                bucket.labels['id']: {"data": {"name": bucket.name, "storageClass": bucket.storage_class}, "Actions": {
                    "DeteleResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "DELETE"}}} for bucket in
                buckets}

        resp = make_response(buckets_dicts)
        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"

    elif request.method == "POST":

        request_json = request.json

        if "name" not in request_json.keys() or "storageClass" not in request_json.keys():
            return bad_request(request.method, request.path)

        bucket_id = random.randint(1, int(2 ** 64))
        buckets = storage_client.list_buckets()

        if "id" in [label for label in [bkt.labels for bkt in buckets]]:
            bucket_ids = [i.labels["id"] for i in buckets]
        else:
            bucket_ids = []

        while bucket_id in bucket_ids:
            bucket_id = random.randint(1, int(2 ** 64))

        bucket = storage_client.bucket(request_json["name"])
        bucket.storage_class = request_json["storageClass"]

        bucket.labels = {"id": str(bucket_id)}

        print(bucket.labels)
        new_bucket = storage_client.create_bucket(bucket, location="europe-west3")

        response_dict = {"id": bucket.labels["id"], "Actions": {
            "DeleteResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "DELETE"}
            , "ReplaceResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "PUT"}
            , "GetResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "GET"}}}

        print(response_dict)

        resp = make_response(response_dict)

        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"
    return resp


@app.route('/buckets/<id>', methods=["GET", "PUT", "POST", "DELETE"])
def bucket(id):
    if request.method not in ["GET", "PUT", "POST", "DELETE"]:
        return method_not_allowed(request.method, request.path, ["GET", "PUT", "POST", "DELETE"])


    if request.method == "GET":

        bucket = getBucket(id)
        if bucket == []:
            bucket_dicts = {}
        else:
            buckets_dicts = {
                bucket.labels['id']: {"data": {"name": bucket.name, "storageClass": bucket.storage_class}, "Actions": {
                    "DeteleResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "DELETE"}}}
            }
        resp = make_response(buckets_dicts)
        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"

    # pun pe id ul specificat
    if request.method == "PUT":
        request_json = request.json
        if "name" not in request_json.keys() or "storageClass" not in request_json.keys():
            return bad_request(request.method, request.path)

        deleteBucket(id)
        bucket = postBucket(request_json["name"], request_json["storageClass"], id)
        bucket_dicts = getBucketInfo(bucket)
        resp=make_response(bucket_dicts)


    # ignore
    if request.method == "POST":
        pass

    if request.method == "DELETE":
        resp = deleteBucket(id)
        pass

    return resp


@app.route("/buckets/<int:bct_id>/objects", methods=["GET", "POST"])
def objects(bct_id):
    if request.method not in ["GET", "POST"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    response = {}

    if request.method == "GET":

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)

        if bucket == -1:
            return not_found(f"{bct_id}", request.path)

        blobs = storage_client.list_blobs(bucket.name)
        blobs = list(filter(lambda x: x.metadata is not None and "id" in x.metadata.keys(), blobs))

        if blobs:
            blobs_dict = {blob.metadata["id"]: {"data": {"name": blob.name, "mime-type": blob.content_type,
                                                         "content": blob.download_as_bytes().decode()}, "Actions": {}}
                          for blob in blobs}
        else:
            blobs_dict = {}

        response = make_response(blobs_dict)
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"

    elif request.method == "POST":

        if "data" not in request.json.keys() or "name" not in request.json["data"] or "content" not in request.json[
            "data"] or "mime-type" not in request.json["data"]:
            return bad_request(request.method, request.path)

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)

        if bucket == -1:
            return not_found(f"{bct_id}", request.path)

        blob_id = random.randint(1, int(2 ** 64))
        blobs = storage_client.list_blobs(bucket.name)
        blob_ids = []
        if "id" in [meta for meta in [blob.metadata for blob in blobs]]:
            blob_ids = [i.metadata["id"] for i in blobs]

        while blob_id in blob_ids:
            blob_id = random.randint(1, int(2 ** 64))

        blob_object = bucket.blob(request.json["data"]["name"])
        blob_object.metadata = {"id": blob_id}

        # print(f"{request.json['data']['content']}  {type(request.json['data']['content'])}")

        blob_object.upload_from_string(request.json["data"]["content"], content_type=request.json["data"]["mime-type"])

        response_dict = {"id": blob_object.metadata["id"], "Actions": {
            "DeleteResource": {"URL": f"/buckets/{blob_object.metadata['id']}", "Method": "DELETE"}
            , "ReplaceResource": {"URL": f"/buckets/{blob_object.metadata['id']}", "Method": "PUT"}
            , "GetResource": {"URL": f"/buckets/{blob_object.metadata['id']}", "Method": "GET"}}}

        print(response_dict)
        response = make_response(response_dict)
        response.status_code = 201
        response.headers["Content-Type"] = "application/json"

    return response


@app.route('/buckets/<int:bkt_id>/objects/<int:obj_id>', methods=["GET", "PUT", "DELETE"])
def object(bkt_id, obj_id):
    if request.method not in ["GET", "PUT", "DELETE"]:
        return method_not_allowed(request.method, request.path, ["GET", "PUT", "DELETE"])

    response = {}

    if request.method == "GET":

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bkt_id, buckets), -1)
        # bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)
        if bucket == -1:
            return not_found(f"{bkt_id}", request.path)

        blobs = storage_client.list_blobs(bucket.name)
        blob = next(filter(lambda x: x.metadata is not None and "id" in x.metadata.keys() and int(x.metadata["id"]) == obj_id, blobs), -1)

        if blob != -1:
            blobs_dict = {blob.metadata["id"]: {"data": {"name": blob.name, "mime-type": blob.content_type, "content": blob.download_as_bytes().decode()}, "Actions": {}}}
        else:
            blobs_dict = {}

        response = make_response(blobs_dict)
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"

    elif request.method == "DELETE":

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bkt_id, buckets), -1)
        # bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)
        if bucket == -1:
            return not_found(f"{bkt_id}", request.path)

        blobs = storage_client.list_blobs(bucket.name)
        blob = next(filter(lambda x: x.metadata is not None and "id" in x.metadata.keys() and int(x.metadata["id"]) == obj_id, blobs), -1)

        if blob != -1:
            blob.delete()
        else:
            return not_found(f"Object {obj_id}", request.path)

    return response



if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080)
