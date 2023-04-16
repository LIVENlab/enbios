# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(['service_main.py'],
             pathex=['/home/rnebot/Dropbox/nis-backend/nexinfosys/restful_service'],
             binaries=[],
             datas=[('../frontend/', './nexinfosys/frontend'), ('/home/rnebot/anaconda3/lib/python3.7/site-packages/pandasdmx/agencies.json', 'pandasdmx')],
             hiddenimports=['nexinfosys.command_executors.version2', 'nexinfosys.command_executors.version2.attribute_sets_command', 'nexinfosys.command_executors.version2.dataset_data_command', 'nexinfosys.command_executors.version2.dataset_definition_command', 'nexinfosys.command_executors.version2.dataset_query_command', 'nexinfosys.command_executors.version2.hierarchy_categories_command', 'nexinfosys.command_executors.version2.hierarchy_mapping_command', 'nexinfosys.command_executors.version2.interfaces_command', 'nexinfosys.command_executors.version2.interface_types_command', 'nexinfosys.command_executors.version2.matrix_indicators_command', 'nexinfosys.command_executors.version2.nested_commands_command', 'nexinfosys.command_executors.version2.pedigree_matrices_command', 'nexinfosys.command_executors.version2.problem_statement_command', 'nexinfosys.command_executors.version2.processor_scalings_command', 'nexinfosys.command_executors.version2.processors_command', 'nexinfosys.command_executors.version2.references_v2_command', 'nexinfosys.command_executors.version2.relationships_command', 'nexinfosys.command_executors.version2.scalar_indicator_benchmarks_command', 'nexinfosys.command_executors.version2.scalar_indicators_command', 'nexinfosys.command_executors.version2.scale_conversion_v2_command', 'nexinfosys.command_executors.specification', 'nexinfosys.command_executors.specification.data_input_command', 'nexinfosys.command_executors.specification.hierarchy_command', 'nexinfosys.command_executors.specification.metadata_command', 'nexinfosys.command_executors.specification.pedigree_matrix_command', 'nexinfosys.command_executors.specification.references_command', 'nexinfosys.command_executors.specification.scale_conversion_command', 'nexinfosys.command_executors.specification.structure_command', 'nexinfosys.command_executors.specification.upscale_command', 'nexinfosys.command_executors.external_data', 'nexinfosys.command_executors.external_data.etl_external_dataset_command', 'nexinfosys.command_executors.external_data.mapping_command', 'nexinfosys.command_executors.external_data.parameters_command'],
             hookspath=[],
             runtime_hooks=[],
             excludes=['matplotlib', '_tkinter', 'PyQt4', 'PyQt5', 'IPython'],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='nis-backend',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True )
