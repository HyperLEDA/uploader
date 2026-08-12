function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function extractUiSchema(
  schema: Record<string, unknown>,
): Record<string, unknown> {
  const ui: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(schema)) {
    if (key.startsWith("ui:")) {
      ui[key] = value;
    }
  }
  const properties = schema.properties;
  if (isRecord(properties)) {
    for (const [name, sub] of Object.entries(properties)) {
      if (!isRecord(sub)) {
        continue;
      }
      const nested = extractUiSchema(sub);
      if (Object.keys(nested).length > 0) {
        ui[name] = nested;
      }
    }
  }
  const items = schema.items;
  if (isRecord(items)) {
    const nested = extractUiSchema(items);
    if (Object.keys(nested).length > 0) {
      ui.items = nested;
    }
  }
  return ui;
}
