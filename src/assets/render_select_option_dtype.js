var dmcfuncs = window.dashMantineFunctions = window.dashMantineFunctions || {};
var dmc = window.dash_mantine_components;
var iconify = window.dash_iconify;

// datatype is encoded in the option.value string as: "column_name<<datatype>>"
// extract it with regex
const dtypePattern = /^.+<<(str|category|int|float|bool|date|datetime)>>$/;
function extractDtype(columnKey) {
    const match = columnKey.match(dtypePattern);
    return match ? match[1] : null;
}
const size = 18;
const nullIcon = React.createElement(iconify.DashIconify, { icon: "pixel:question-solid", width: size });
const icons = {
    str: React.createElement(iconify.DashIconify, { icon: "radix-icons:text", width: size }),
    category: React.createElement(iconify.DashIconify, { icon: "material-symbols:category-outline", width: size }),
    int: React.createElement(iconify.DashIconify, { icon: "carbon:string-integer", width: size }),
    float: React.createElement(iconify.DashIconify, { icon: "lsicon:decimal-filled", width: size }),
    bool: React.createElement(iconify.DashIconify, { icon: "ix:data-type-boolean", width: size }),
    date: React.createElement(iconify.DashIconify, { icon: "fluent-mdl2:event-date", width: size }),
    datetime: React.createElement(iconify.DashIconify, { icon: "fluent-mdl2:date-time", width: size }),
};

dmcfuncs.renderSelectOptionDtype = function ({ side, option, checked }) {
  return React.createElement(
    dmc.Group,
    {
        flex: "1",
        gap: "xs",
        justify: (side === "right" ? "space-between" : "flex-start"),
    },
    side === "left" ? (extractDtype(option.value) ? icons[extractDtype(option.value)] : nullIcon) : option.label,
    side === "right" ? (extractDtype(option.value) ? icons[extractDtype(option.value)] : nullIcon) : option.label
  );
};

dmcfuncs.renderSelectOptionDtypeLeft = function ({ option, checked }) {
    return dmcfuncs.renderSelectOptionDtype({ side: "left", option, checked });
};

dmcfuncs.renderSelectOptionDtypeRight = function ({ option, checked }) {
    return dmcfuncs.renderSelectOptionDtype({ side: "right", option, checked });
};

