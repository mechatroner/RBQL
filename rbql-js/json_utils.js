// JsonFinalizer is 100% AI-generated to my spec.

class JsonFinalizer {
  constructor() {
    this.buffer = "";
    
    // Parser state
    this.processedIndex = 0; // Next character index to parse in this.buffer
    this.stack = [];          // Tracks open '{' and '['
    this.inString = false;    // Whether we are currently inside a JSON string
    this.isEscaped = false;   // Whether the current char in string is preceded by '\'
    
    // First object status tracker
    this.firstObjStartIndex = -1;
    this.firstObjEndIndex = -1;
    this.hasInvalidPrefix = false;
  }

  /**
   * Adds string data to the internal buffer and updates the parser state.
   */
  add_data(s) {
    if (typeof s !== 'string' || s.length === 0) return;

    this.buffer += s;
    this._parseBuffer();
  }

  /**
   * Returns the first full JSON object/array from the buffer starting from index 0.
   * Throws an error if the prefix cannot be a valid JSON start.
   * Returns null if no complete object is ready yet.
   */
  get_first_object() {
    if (this.hasInvalidPrefix) {
      throw new Error("Invalid prefix: Buffer does not start with a valid JSON object or array.");
    }

    if (this.firstObjStartIndex !== -1 && this.firstObjEndIndex !== -1) {
      return this.buffer.slice(this.firstObjStartIndex, this.firstObjEndIndex + 1);
    }

    return null;
  }

  /**
   * Deletes the first full object chunk from the internal buffer and resets/recalculates state.
   */
  erase_first_object() {
    if (this.firstObjStartIndex === -1 || this.firstObjEndIndex === -1) {
      return; // No object ready to erase
    }

    // Drop the parsed first object from buffer
    this.buffer = this.buffer.slice(this.firstObjEndIndex + 1);
    
    // Reset state and re-evaluate remaining buffer
    this._resetState();
    this._parseBuffer();
  }

  /**
   * Resets all internal parsing pointers and states.
   */
  _resetState() {
    this.processedIndex = 0;
    this.stack = [];
    this.inString = false;
    this.isEscaped = false;
    this.firstObjStartIndex = -1;
    this.firstObjEndIndex = -1;
    this.hasInvalidPrefix = false;
  }

  /**
   * Incremental parser machine.
   */
  _parseBuffer() {
    // If invalid prefix was previously found or object is already complete, pause processing.
    if (this.hasInvalidPrefix || this.firstObjEndIndex !== -1) {
      return;
    }

    for (; this.processedIndex < this.buffer.length; this.processedIndex++) {
      const char = this.buffer[this.processedIndex];

      // Handle whitespace before finding the initial open bracket
      if (this.firstObjStartIndex === -1) {
        if (/\s/.test(char)) {
          continue; // Skip leading whitespace
        }
        
        // A JSON root object/array must start with '{' or '['
        if (char === '{' || char === '[') {
          this.firstObjStartIndex = this.processedIndex;
          this.stack.push(char);
          continue;
        } else {
          this.hasInvalidPrefix = true;
          return;
        }
      }

      // --- Inside the JSON structure ---
      if (this.inString) {
        if (this.isEscaped) {
          this.isEscaped = false;
        } else if (char === '\\') {
          this.isEscaped = true;
        } else if (char === '"') {
          this.inString = false;
        }
        continue;
      }

      // Outside strings: process structural characters
      switch (char) {
        case '"':
          this.inString = true;
          break;

        case '{':
        case '[':
          this.stack.push(char);
          break;

        case '}':
          if (this.stack.length === 0 || this.stack[this.stack.length - 1] !== '{') {
            this.hasInvalidPrefix = true;
            return;
          }
          this.stack.pop();
          break;

        case ']':
          if (this.stack.length === 0 || this.stack[this.stack.length - 1] !== '[') {
            this.hasInvalidPrefix = true;
            return;
          }
          this.stack.pop();
          break;
      }

      // Check if we just closed the root object
      if (this.stack.length === 0) {
        this.firstObjEndIndex = this.processedIndex;
        this.processedIndex++; // Advance past end boundary for next run
        return;
      }
    }
  }
}

module.exports.JsonFinalizer = JsonFinalizer;
