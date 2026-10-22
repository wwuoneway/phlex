// Round trip of each event's own layer numbers through a FORM file. With runs == 0 the events sit
// directly under the job (job -> event); otherwise 200 events are split over runs (job -> run -> event).
local layers(starting_number, runs) =
  if runs == 0 then {
    event: { parent: 'job', total: 200, starting_number: starting_number },
  } else {
    run: { parent: 'job', total: runs, starting_number: starting_number },
    event: { parent: 'run', total: 200 / runs, starting_number: starting_number },
  };

{
  write(output_file, starting_number, runs=0):: {
    driver: { cpp: 'generate_layers', layers: layers(starting_number, runs) },
    sources: {
      numbers: { cpp: 'cell_number_vector', creator: 'numbers' },
    },
    modules: {
      stamper: { cpp: 'cell_number_vector', input_creator: 'numbers' },
      output: { cpp: 'form_module', output_file: output_file, products: ['cell_numbers'] },
    },
  },

  read(input_file, starting_number, runs=0):: {
    driver: { cpp: 'generate_layers', layers: layers(starting_number, runs) },
    sources: {
      form_in: {
        cpp: 'form_source',
        stage: 'test',
        input_file: input_file,
        plugin: 'stamper',
        algorithm: 'stamp',
        creator: 'stamper',
        products: ['cell_numbers'],
      },
    },
    modules: {
      check: { cpp: 'cell_number_vector', input_creator: 'stamper', check: true },
    },
  },
}
