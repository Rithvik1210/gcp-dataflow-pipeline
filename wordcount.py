import apache_beam as beam

with beam.Pipeline() as p:
    (
        p
        | 'Read' >> beam.Create(['Hello world', 'Hello Beam'])
        | 'Split' >> beam.FlatMap(lambda x: x.split())
        | 'Pair' >> beam.Map(lambda word: (word, 1))
        | 'Count' >> beam.CombinePerKey(sum)
        | 'Print' >> beam.Map(print)
    )
