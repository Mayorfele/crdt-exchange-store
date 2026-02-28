use shared::types::EntryValue;

pub fn new(value: String) -> EntryValue {
    EntryValue::Register(value)
}

pub fn read(value: &EntryValue) -> Option<&str> {
    if let EntryValue::Register(v) = value {
        Some(v.as_str())
    } else {
        None
    }
}

pub fn write(value: &mut EntryValue, new_value: String) {
    *value = EntryValue::Register(new_value);
}