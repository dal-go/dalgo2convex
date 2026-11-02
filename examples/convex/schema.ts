import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  tasks: defineTable({
    tenant: v.string(), title: v.string(), amount: v.int64(),
    bytes: v.bytes(), score: v.number(), done: v.boolean(),
  }).index("by_tenant", ["tenant"]),
});
