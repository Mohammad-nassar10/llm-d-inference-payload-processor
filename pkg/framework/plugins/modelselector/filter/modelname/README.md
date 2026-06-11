# Model Name Filter

Restricts the candidate models to the model name(s) in the request body. The body's
model field is treated as a single model name, with a special case allowing the name
to be an array of model names.

It is registered as type `model-name-filter` and runs as a modelselector filter.

## What it does

1. Reads the model field from the request body (`model` by default).
2. When the field holds a configured model name in the data store, it is added as a candidate, and models would include a single model.
3. When the field holds a JSON style array, the array is interpreted as "choose from the list". The filter adds to models the ones configured in the data store, dropping those that are not in the data store. The scorers and picker select the best of the requested subset, and the model-selector plugin outputs the selected model into the `model` field.
4. If the field is absent, an empty string, or an empty array, all models in the data store are considered as candidates.
5. If the filter is not able to add any model, that is, all named models are not in the data store, or the field is malformed (not a string or an array of non-empty strings), the pipeline rejects the request with HTTP 400.

## Inputs consumed

- The configured model field of the request body.
- The candidate model list from the datalayer.

## Configuration

```json
{"requestModelField": "model"}
```

- `requestModelField` (optional): the request-body field holding the requested model name. Defaults to `model`.
