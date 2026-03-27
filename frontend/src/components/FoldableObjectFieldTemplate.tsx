import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Accordion from "@mui/material/Accordion";
import AccordionDetails from "@mui/material/AccordionDetails";
import AccordionSummary from "@mui/material/AccordionSummary";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import type { ObjectFieldTemplateProps } from "@rjsf/utils";

export function FoldableObjectFieldTemplate(props: ObjectFieldTemplateProps) {
  const isRoot = props.fieldPathId.$id === "root";
  const title = props.title || "Section";

  const content = (
    <Box>
      {props.description}
      {props.properties.map((property) => (
        <Box key={property.name} sx={{ mb: 2 }}>
          {property.content}
        </Box>
      ))}
    </Box>
  );

  if (isRoot) {
    return content;
  }

  return (
    <Accordion disableGutters>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Typography variant="subtitle1">{title}</Typography>
      </AccordionSummary>
      <AccordionDetails>{content}</AccordionDetails>
    </Accordion>
  );
}
