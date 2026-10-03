// Writes OUT_DIR/<Type>.json, the JSON Schema of each operator.yaml type (in or out) the operator's assembly
// declares: its output types, and input types it owns because they arrive via Input (not ModelGenerator's classes in
// <namespace>.Models.In, of types other operators own). Same System.Text.Json
// settings the operators serialize with (the defaults, plus the types' own attributes).
//
// With a schema-registry checkout, types it marks ignoreSchema: true are skipped.
//
//     dotnet run --project operator/schema/SchemaGen -- OPERATOR_DLL OPERATOR_YAML OUT_DIR [SCHEMA_REGISTRY_DIR]
using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;
using NJsonSchema;
using NJsonSchema.Generation;
using YamlDotNet.RepresentationModel;

if (args.Length is < 3 or > 4)
{
    Console.Error.WriteLine("usage: SchemaGen OPERATOR_DLL OPERATOR_YAML OUT_DIR [SCHEMA_REGISTRY_DIR]");
    return 2;
}
var (dll, yamlPath, outDir) = (Path.GetFullPath(args[0]), args[1], args[2]);
var registry = args.Length == 4 ? args[3] : null;

// The operator's dependencies resolve from its build output.
var resolver = new AssemblyDependencyResolver(dll);
AssemblyLoadContext.Default.Resolving += (ctx, name) =>
    resolver.ResolveAssemblyToPath(name) is { } path ? ctx.LoadFromAssemblyPath(path) : null;
var assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(dll);
Type[] types;
try { types = assembly.GetTypes(); }
catch (ReflectionTypeLoadException e) { types = e.Types.OfType<Type>().ToArray(); }

var yaml = new YamlStream();
yaml.Load(new StringReader(File.ReadAllText(yamlPath)));
var root = (YamlMappingNode)yaml.Documents[0].RootNode;
var names = new[] { "in", "out" }
    .SelectMany(key => root.Children.TryGetValue(key, out var list) ? ((YamlSequenceNode)list).Select(n => n.ToString()) : [])
    .Distinct();

var settings = new SystemTextJsonSchemaGeneratorSettings { SerializerOptions = new JsonSerializerOptions() };

Directory.CreateDirectory(outDir);
var errors = 0;
foreach (var name in names)
{
    if (registry is not null && IgnoresSchema(Path.Combine(registry, name, "metadata.yaml")))
    {
        Console.WriteLine($"skip: {name}: ignoreSchema in schema-registry");
        continue;
    }
    var matches = types.Where(t => t.Name == name && t.Namespace?.EndsWith(".Models.In") != true).ToArray();
    if (matches.Length > 1)
    {
        Console.Error.WriteLine($"error: {name}: several classes named so ({string.Join(", ", matches.Select(t => t.FullName))})");
        errors++;
        continue;
    }
    if (matches.Length == 0)
    {
        Console.WriteLine($"skip: {name}: not declared here (another operator owns it)");
        continue;
    }
    var schema = JsonSchema.FromType(matches[0], settings);
    schema.Title = name;
    File.WriteAllText(Path.Combine(outDir, $"{name}.json"), schema.ToJson());
    Console.WriteLine($"ok: {name}: {matches[0].FullName}");
}
return errors == 0 ? 0 : 1;

static bool IgnoresSchema(string metadata)
{
    if (!File.Exists(metadata))
        return false;
    var doc = new YamlStream();
    doc.Load(new StringReader(File.ReadAllText(metadata)));
    return doc.Documents.Count > 0 && doc.Documents[0].RootNode is YamlMappingNode map
        && map.Children.TryGetValue("ignoreSchema", out var v) && v.ToString() == "true";
}
