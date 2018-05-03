import sys

from flask import Flask, abort, jsonify, request

from flask_restful import Api, Resource
from sqlalchemy.exc import IntegrityError

from db.tables import AnnotationType, TimerangeAnnotation, TimestampAnnotation
from storage import ImmutableStore, MutableStore

immutable_store = ImmutableStore(location='test_db')
mutable_store = MutableStore()


class App:
    app = Flask(__name__)
    api = Api(app)

    def add_mutable_resource_class(self, c, name, url, **kwargs):
        self.api.add_resource(c, url + "/<string:id>", **kwargs)

        class WrapClass(c):
            def get(self, *ar, **kw):
                return c._all(self, *ar, **kw)

            def post(self, *ar, **kw):
                return c.post(self, *ar, **kw)

        WrapClass.__name__ = name + 'WrapClass'

        self.api.add_resource(WrapClass, url, **kwargs)

    def add_immutable_resource_class(self, c, name, url, **kwargs):
        self.api.add_resource(c, url, **kwargs)

    def run(self, host=None, port=None, debug=None, **options):
        self.app.run(host=host, port=port, debug=debug, **options)


app = App()


def get_object_or_404(model, **kwargs):
    result = mutable_store.get(model, **kwargs)
    if result is None:
        abort(404)
    return result


def get_json_or_404(model, **kwargs):
    return get_object_or_404(model, **kwargs).to_json()


class MutableResource(Resource):
    model = None

    def _all(self):
        return [o.to_json() for o in mutable_store.get_all(self.model)]

    def _post(self, **kwargs):
        try:
            o = mutable_store.create(self.model, **kwargs)
        except IntegrityError:
            return '', 409
        return o.to_json(), 201

    def _put(self, id, **kwargs):
        try:
            o = mutable_store.update(self.model, id, **kwargs)
        except IntegrityError:
            abort(404)

        return o.to_json()

    def _get(self, **kwargs):
        return jsonify(get_json_or_404(self.model, **kwargs))

    def _delete(self, id):
        mutable_store.delete(self.model, id=id)
        return '', 204


class AnnotationTypeResource(MutableResource):
    model = AnnotationType

    def post(self):
        data = {
            'name': request.form['name']
        }
        return super()._post(**data)

    def put(self, id):
        data = {
            'name': request.form['name']
        }
        return super()._put(id, **data)

    def get(self, id):
        return super()._get(id=id)

    def delete(self, id):
        return super()._delete(id=id)


class TimestampAnnotationResource(MutableResource):
    model = TimestampAnnotation

    def post(self):
        data = {
            'source_id': request.form['source_id'],
            'type_id': request.form['type_id'],
            'value': request.form.get('value', 0.),
            'comment': request.form.get('comment', ''),
            'timestamp_micros': request.form['timestamp_micros'],
        }
        return super()._post(**data)

    def put(self, id):
        data = {
            'source_id': request.form.get('source_id', None),
            'type_id': request.form.get('type_id', None),
            'value': request.form.get('value', None),
            'comment': request.form.get('comment', None),
            'timestamp_micros': request.form.get('timestamp_micros', None),
        }
        return super()._put(id=id, **data)

    def get(self, id):
        return super()._get(id=id)

    def delete(self, id):
        return super()._delete(id=id)


class TimerangeAnnotationResource(MutableResource):
    model = TimerangeAnnotation

    def post(self):
        data = {
            'source_id': request.form['source_id'],
            'type_id': request.form['type_id'],
            'value': request.form.get('value', 0.),
            'comment': request.form.get('comment', ''),
            'start_micros': request.form['start_micros'],
            'end_micros': request.form['end_micros'],
        }
        return super()._post(**data)

    def put(self, id):
        data = {
            'source_id': request.form.get('source_id', None),
            'type_id': request.form.get('type_id', None),
            'value': request.form.get('value', None),
            'comment': request.form.get('comment', None),
            'start_micros': request.form.get('start_micros', None),
            'end_micros': request.form.get('end_micros', None),
        }
        return super()._put(id=id, **data)

    def get(self, id):
        return super()._get(id=id)

    def delete(self, id):
        return super()._delete(id=id)


class WaveformResource(Resource):
    def put(self):
        abort(404)

    def post(self):
        try:
            if 'hf_source_id' in request.form:
                immutable_store.write_lf(
                    source_id=request.form['hf_source_id'],
                    type_id=request.form['hf_type_id'],
                    timestamp_micros=request.form['hf_timestamp_micros'],
                    value=request.form.get['hf_value'],
                )
            else:
                immutable_store.write_hf(
                    source_id=request.form['hf_source_id'],
                    type_id=request.form['hf_type_id'],
                    start_micros=request.form['hf_start_micros'],
                    frequency=request.form['hf_value'],
                    values=request.form['hf_values']
                )
        except KeyError:
            abort(400)
        return '', 201

    def get(self):
        try:
            source_id = request.args.get('source_id', None)
            type_id = request.args.get('type_id', None)
            lf = request.args['lf'] == 'true'
            hf = request.args['hf'] == 'true'
            start_micros = int(request.args['start_micros'])
            end_micros = int(request.args['end_micros'])
        except KeyError:
            abort(400)

        filters = {}
        if source_id is not None:
            filters['source_id'] = source_id
        if type_id is not None:
            filters['type_id'] = type_id
        return immutable_store.read_all_blocks(start_micros=start_micros, end_micros=end_micros, lf=lf, hf=hf, **filters)

    def delete(self, id):
        abort(404)


app.add_mutable_resource_class(AnnotationTypeResource, 'at', '/annotations/types')
app.add_mutable_resource_class(TimestampAnnotationResource, 'ts', '/annotations/timestamp')
app.add_mutable_resource_class(TimerangeAnnotationResource, 'tr', '/annotations/timerange')
app.add_immutable_resource_class(WaveformResource, 'wf', '/waveforms')

# * [x] Get data from date A to date B
# * [x] Get data where bed_id=X, signal_type=ECG
# * [ ] Get data where there are arythmia annotations
#

if __name__ == "__main__":
    app.run(sys.argv[1], int(sys.argv[2]))
