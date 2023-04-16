from dotted.collection import DottedDict
import nexinfosys
from nexinfosys.command_definitions import commands
from nexinfosys.command_descriptions import c_descriptions
from nexinfosys.command_field_definitions import command_fields
from nexinfosys.command_field_descriptions import cf_descriptions
from nexinfosys.command_generators.parser_field_examples import generic_field_examples, generic_field_syntax


def obtain_commands_help(format: str):
    def field2html(f):
        return f"<li><b>{f.name}</b> ({'<b>mandatory</b>' if f.mandatory else 'optional'})" \
               f"<p>{f.description}</p>" \
               f"<p><b>Syntax:</b> {f.syntax}</p>" \
               f"{'<p><b>Examples:</b> ' +', '.join(f.examples) + '</p>' if f.examples else ''}" \
               f"</li>"
    # Obtain JSON for each command
    d = []
    sequence = [nexinfosys.CommandType.core,
                nexinfosys.CommandType.input,
                nexinfosys.CommandType.analysis,
                nexinfosys.CommandType.metadata,
                nexinfosys.CommandType.convenience,
                nexinfosys.CommandType.misc]
    for ctype in sequence:
        for cmd in commands:
            if cmd.is_v2 and cmd.cmd_type == ctype:
                d.append(obtain_command_help(cmd))

    # Convert JSON to the specific format
    res = "<!DOCTYPE html><html><body>"
    prev_type = None
    for c1 in d:
        if not c1:
            continue
        cmd = DottedDict(c1)
        if format == "html":
            # Each type is H1
            if cmd.type != prev_type:
                prev_type = cmd.type
                res += f"<h1>{cmd.type} commands</h1>"
            # A command

            res += f"<h2>{cmd.title} ('{cmd.name}')</h2>" \
                   f"<p>{cmd.description}</p>" \
                   f"<h3>Semantics</h3><p>{cmd.semantics}</p>" \
                   f"<h3>Fields</h3><ul>{''.join([field2html(DottedDict(f1)) for f1 in cmd.fields])}</ul>"

    res += "</body></html>"

    return res


def obtain_command_help(cmd: nexinfosys.Command):
    if c_descriptions.get((cmd.name, "title")):
        cmdflds = command_fields.get(cmd.name, None)
        return dict(type=cmd.cmd_type,
                    name=cmd.allowed_names[0],
                    allowed_names=cmd.allowed_names,
                    internal_name=cmd.name,
                    title=c_descriptions[(cmd.name, "title")],
                    description=c_descriptions[(cmd.name, "description")],
                    semantics=c_descriptions[(cmd.name, "semantics")],
                    examples=c_descriptions[(cmd.name, "examples")],
                    examples2=cmd.direct_examples,
                    template="\t".join([f.allowed_names[0] for f in cmdflds if "@" not in f.allowed_names[0]]) if cmdflds else "",
                    files=cmd.files,
                    fields=[obtain_command_field_help(cmd.name, f) for f in cmdflds if "@" not in f.allowed_names[0]] if cmdflds else [],
                    )
    else:
        return None


def obtain_command_field_help(cmd: str, f: nexinfosys.CommandField):
    desc = cf_descriptions.get((cmd, f.name), f"Text for field {f.name} in command {cmd}")
    if isinstance(desc, list):
        desc = desc[1]
    return dict(name=f.allowed_names[0],
                internal_name=f.name,
                other_names=f.allowed_names[1:] if len(f.allowed_names)>1 else [],
                mandatory=f.mandatory,
                description=desc,
                examples=generic_field_examples[f.parser] if generic_field_examples.get(f.parser) else [],
                syntax='One of: '+', '.join(f.allowed_values) if f.allowed_values else generic_field_syntax.get(f.parser, "<>")
                )


# if __name__ == '__main__':
#     o = obtain_commands_help("html")
#     with open("/home/rnebot/Downloads/help.html", "wt") as text_file:
#         text_file.write(o)
