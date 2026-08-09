"use server";

import { signIn } from "@/auth";
import { AuthError } from "next-auth";

export async function login(_state: string | undefined, formData: FormData): Promise<string | undefined> {
  try {
    await signIn("credentials", { username: formData.get("username"), password: formData.get("password"), redirectTo: "/" });
  } catch (error) {
    if (error instanceof AuthError) return "Invalid username or password.";
    throw error;
  }
}
