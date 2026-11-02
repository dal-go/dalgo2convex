import { query } from "./_generated/server";
import { paginationOptsValidator } from "convex/server";
import { v } from "convex/values";

/** Ordinary PUBLIC queries. Deploy under your own reviewed auth configuration. */
export const getTask = query({
  args: { id: v.id("tasks"), tenant: v.string() },
  handler: async (ctx, args) => {
    const identity = await ctx.auth.getUserIdentity();
    if (identity === null || identity.subject !== args.tenant) throw new Error("denied");
    const row = await ctx.db.get("tasks", args.id);
    if (row !== null && row.tenant !== identity.subject) throw new Error("denied");
    return row;
  },
});
export const listTasks = query({
  args: { tenant: v.string(), paginationOpts: paginationOptsValidator },
  handler: async (ctx, args) => {
    const identity = await ctx.auth.getUserIdentity();
    if (identity === null || identity.subject !== args.tenant) throw new Error("denied");
    return await ctx.db.query("tasks")
      .withIndex("by_tenant", q => q.eq("tenant", identity.subject))
      .order("asc").paginate({
        ...args.paginationOpts,
        numItems: Math.min(args.paginationOpts.numItems, 1000),
        maximumRowsRead: Math.min(args.paginationOpts.maximumRowsRead ?? 1000, 1000),
        maximumBytesRead: Math.min(args.paginationOpts.maximumBytesRead ?? 1_048_576, 1_048_576),
      });
  },
});
