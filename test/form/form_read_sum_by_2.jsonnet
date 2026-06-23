{
  driver: {
    cpp: 'generate_layers',
    layers: {
      event: { parent: 'job', total: 10, starting_number: 0 },
    },
  },
  sources: {
    form_input_test: {
      cpp: 'form_source',
      input_file: 'output.root',
      products: ['sum'],
      cell_number: 8, // one cell  
      // optional: advertised creator: if not set, use ProductRegistry.producer
      advertised_creator: 'multiply_sum_by_2',      
    },
  },
  modules: {
    sum_by_2: {
      cpp: 'sum_by_2_transform',
      layer: 'event',
      creator: 'multiply_sum_by_2',
      output_suffix: '',
    },
    // Output module write data products to FORM file
    output: {
      cpp: 'form_module', // FORM module for writing
      // List of product names to write to output
      // Must match the 'suffix' defined in the source/product registration
      products: ['sum_by_2'],
      // output_file and technology can be specified here
      output_file: 'new_output.root',
      technology: 'ROOT_TTREE',
    },
  },
}
